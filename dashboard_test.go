package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDashboardStartsMappingsInStartingState(t *testing.T) {
	model := newTestDashboard(t, portMappings{{localPort: 8080, workstationPort: 80}}, probeTCP)

	assert.Equal(t, []mappingStatus{{
		mapping: portMapping{localPort: 8080, workstationPort: 80},
		probe:   portMapping{localPort: 8080, workstationPort: 80},
		status:  statusStarting,
	}}, model.mappings)
}

func TestDashboardUpdatesProbeStatuses(t *testing.T) {
	model := newTestDashboard(t, portMappings{
		{localPort: 8080, workstationPort: 80},
		{localPort: 5432, workstationPort: 5432},
	}, probeTCP)

	updated, _ := model.Update(testProbeResults(
		probeResult{index: 0},
		probeResult{index: 1, err: errors.New("connection refused")},
	))

	assert.Equal(t, []mappingStatus{
		{
			mapping:     portMapping{localPort: 8080, workstationPort: 80},
			probe:       portMapping{localPort: 8080, workstationPort: 80},
			status:      statusOpen,
			hasBeenOpen: true,
		},
		{
			mapping:                  portMapping{localPort: 5432, workstationPort: 5432},
			probe:                    portMapping{localPort: 5432, workstationPort: 5432},
			status:                   statusWaiting,
			consecutiveProbeFailures: 1,
		},
	}, updated.(dashboardModel).mappings)
}

func TestDashboardRestartsTunnelsAfterThreeFailedProbes(t *testing.T) {
	restartRequests := make(chan struct{}, 1)
	model := newDashboard(dashboardConfig{
		ctx:             t.Context(),
		mappings:        portMappings{{localPort: 8080, workstationPort: 80}},
		probeMappings:   portMappings{{localPort: 50000, workstationPort: 80}},
		probe:           probeTCP,
		restartRequests: restartRequests,
	})

	model.updateStatuses(testProbeResults(probeResult{index: 0}))
	for range failedProbesBeforeRestart {
		model.updateStatuses(testProbeResults(probeResult{index: 0, err: errors.New("connection refused")}))
	}

	assert.Equal(t, statusStarting, model.mappings[0].status)
	assert.Equal(t, 0, model.mappings[0].consecutiveProbeFailures)
	assert.Len(t, restartRequests, 1)
}

func TestDashboardDoesNotRestartBeforeTunnelHasOpened(t *testing.T) {
	restartRequests := make(chan struct{}, 1)
	model := newDashboard(dashboardConfig{
		ctx:             t.Context(),
		mappings:        portMappings{{localPort: 8080, workstationPort: 80}},
		probeMappings:   portMappings{{localPort: 50000, workstationPort: 80}},
		probe:           probeTCP,
		restartRequests: restartRequests,
	})

	for range failedProbesBeforeRestart {
		model.updateStatuses(testProbeResults(probeResult{index: 0, err: errors.New("connection refused")}))
	}

	assert.Equal(t, statusWaiting, model.mappings[0].status)
	assert.Empty(t, restartRequests)
}

func TestDashboardSuccessfulProbeRearmsTunnelRestart(t *testing.T) {
	restartRequests := make(chan struct{}, 1)
	model := newDashboard(dashboardConfig{
		ctx:             t.Context(),
		mappings:        portMappings{{localPort: 8080, workstationPort: 80}},
		probeMappings:   portMappings{{localPort: 50000, workstationPort: 80}},
		probe:           probeTCP,
		restartRequests: restartRequests,
	})

	model.updateStatuses(testProbeResults(probeResult{index: 0}))
	for range failedProbesBeforeRestart - 1 {
		model.updateStatuses(testProbeResults(probeResult{index: 0, err: errors.New("connection refused")}))
	}
	model.updateStatuses(testProbeResults(probeResult{index: 0}))
	for range failedProbesBeforeRestart {
		model.updateStatuses(testProbeResults(probeResult{index: 0, err: errors.New("connection refused")}))
	}

	assert.Len(t, restartRequests, 1)
}

func TestDashboardDoesNotRestartReplacementBeforeItHasOpened(t *testing.T) {
	restartRequests := make(chan struct{}, 2)
	model := newDashboard(dashboardConfig{
		ctx:             t.Context(),
		mappings:        portMappings{{localPort: 8080, workstationPort: 80}},
		probeMappings:   portMappings{{localPort: 50000, workstationPort: 80}},
		probe:           probeTCP,
		restartRequests: restartRequests,
	})

	model.updateStatuses(testProbeResults(probeResult{index: 0}))
	for range failedProbesBeforeRestart {
		model.updateStatuses(testProbeResults(probeResult{index: 0, err: errors.New("connection refused")}))
	}
	updated, _ := model.Update(tunnelsRestartedMsg{generation: 1})
	model = updated.(dashboardModel)
	for range failedProbesBeforeRestart {
		model.updateStatuses(testProbeResultsForGeneration(1, probeResult{index: 0, err: errors.New("connection refused")}))
	}

	assert.Len(t, restartRequests, 1)
	assert.Equal(t, statusWaiting, model.mappings[0].status)
}

func TestDashboardDiscardsRemainingProbeResultsAfterRequestingRestart(t *testing.T) {
	restartRequests := make(chan struct{}, 1)
	model := newDashboard(dashboardConfig{
		ctx: t.Context(),
		mappings: portMappings{
			{localPort: 8080, workstationPort: 80},
			{localPort: 5432, workstationPort: 5432},
		},
		probeMappings: portMappings{
			{localPort: 50000, workstationPort: 80},
			{localPort: 50001, workstationPort: 5432},
		},
		probe:           probeTCP,
		restartRequests: restartRequests,
	})
	model.mappings[0].hasBeenOpen = true
	model.mappings[0].consecutiveProbeFailures = failedProbesBeforeRestart - 1

	model.updateStatuses(testProbeResults(
		probeResult{index: 0, err: errors.New("connection refused")},
		probeResult{index: 1},
	))

	assert.Equal(t, statusStarting, model.mappings[1].status)
	assert.False(t, model.mappings[1].hasBeenOpen)
}

func TestDashboardDiscardsProbeTunnelExitFromPreviousGeneration(t *testing.T) {
	model := newTestDashboard(t, portMappings{{localPort: 8080, workstationPort: 80}}, probeTCP)
	model.generation = 1

	updated, _ := model.Update(probeTunnelStoppedMsg{
		index:      0,
		generation: 0,
		err:        errors.New("previous generation stopped"),
	})

	assert.Equal(t, statusStarting, updated.(dashboardModel).mappings[0].status)
	assert.NoError(t, updated.(dashboardModel).probeFailure)
}

func TestDashboardClearsProbeFailureWhenRestarting(t *testing.T) {
	restartRequests := make(chan struct{}, 1)
	model := newDashboard(dashboardConfig{
		ctx:             t.Context(),
		mappings:        portMappings{{localPort: 8080, workstationPort: 80}},
		probeMappings:   portMappings{{localPort: 50000, workstationPort: 80}},
		probe:           probeTCP,
		restartRequests: restartRequests,
	})
	model.probeFailure = errors.New("stale probe failure")

	model.requestRestart()

	assert.NoError(t, model.probeFailure)
}

func TestDashboardSchedulesNextProbeAfterResults(t *testing.T) {
	model := newTestDashboard(t, portMappings{{localPort: 8080, workstationPort: 80}}, probeTCP)

	_, command := model.Update(testProbeResults(probeResult{index: 0}))

	assert.NotNil(t, command)
}

func TestDashboardRunsProbeAfterTick(t *testing.T) {
	model := newTestDashboard(
		t,
		portMappings{{localPort: 8080, workstationPort: 80}},
		func(context.Context, portMapping) error { return nil },
	)

	_, command := model.Update(probeTickMsg{})

	assert.IsType(t, probeResultsMsg{}, command())
}

func TestDashboardStopsMappingsAfterTunnelExit(t *testing.T) {
	model := newTestDashboard(t, portMappings{{localPort: 8080, workstationPort: 80}}, probeTCP)

	updated, command := model.Update(tunnelsStoppedMsg{err: errors.New("connection failed")})

	assert.NotNil(t, command)
	assert.Equal(t, statusStopped, updated.(dashboardModel).mappings[0].status)
	assert.ErrorContains(t, updated.(dashboardModel).failure, "connection failed")
}

func TestDashboardViewIncludesPortMappingAndStatus(t *testing.T) {
	model := newTestDashboard(t, portMappings{{localPort: 8080, workstationPort: 80}}, probeTCP)
	model.mappings[0].status = statusOpen

	view := model.View().Content

	assert.True(t, strings.Contains(view, "localhost:8080"))
	assert.True(t, strings.Contains(view, "workstation:80"))
	assert.True(t, strings.Contains(view, "OPEN"))
}

func TestDashboardMarksMappingUnknownWhenProbeTunnelStops(t *testing.T) {
	model := newTestDashboard(t, portMappings{{localPort: 8080, workstationPort: 80}}, probeTCP)

	updated, _ := model.Update(probeTunnelStoppedMsg{index: 0, err: errors.New("probe tunnel failed")})

	assert.Equal(t, statusUnknown, updated.(dashboardModel).mappings[0].status)
	assert.ErrorContains(t, updated.(dashboardModel).probeFailure, "probe tunnel failed")
}

func TestDashboardPreservesUnknownStatusAfterProbeTunnelStops(t *testing.T) {
	model := newTestDashboard(t, portMappings{{localPort: 8080, workstationPort: 80}}, probeTCP)
	model.mappings[0].status = statusUnknown

	updated, _ := model.Update(testProbeResults(probeResult{index: 0}))

	assert.Equal(t, statusUnknown, updated.(dashboardModel).mappings[0].status)
}

func TestCommandHarnessAllocatesHiddenProbeMappings(t *testing.T) {
	mappings := portMappings{
		{localPort: 8022, workstationPort: 22},
		{localPort: 8080, workstationPort: 8080},
	}
	harness := commandHarness{mappings: mappings, noLemonade: true}

	probeMappings, err := harness.allocateProbeMappings(mappings)

	require.NoError(t, err)
	require.Len(t, probeMappings, 2)
	assert.NotEqual(t, uint16(8022), probeMappings[0].localPort)
	assert.NotEqual(t, uint16(8080), probeMappings[1].localPort)
	assert.NotEqual(t, probeMappings[0].localPort, probeMappings[1].localPort)
	assert.Equal(t, uint16(22), probeMappings[0].workstationPort)
	assert.Equal(t, uint16(8080), probeMappings[1].workstationPort)
}

func TestCommandHarnessDoesNotAllocateProbeMappingForLemonade(t *testing.T) {
	harness := commandHarness{mappings: portMappings{
		{localPort: 8080, workstationPort: 80},
		{localPort: lemonadePort, workstationPort: lemonadePort},
	}}
	monitored := harness.monitoredMappings()

	probeMappings, err := harness.allocateProbeMappings(monitored)

	require.NoError(t, err)
	require.Len(t, probeMappings, 1)
	assert.Equal(t, uint16(80), probeMappings[0].workstationPort)
	assert.NotEqual(t, lemonadePort, probeMappings[0].localPort)
	assert.NotEqual(t, uint16(8080), probeMappings[0].localPort)
}

func TestStartLemonadeIsNoopWhenDisabled(t *testing.T) {
	var calls int
	harness := commandHarness{
		noLemonade: true,
		run: func(context.Context, string, ...string) error {
			calls++
			return nil
		},
	}

	done := harness.startLemonade(t.Context())
	<-done
	assert.Zero(t, calls)
}

func TestStartLemonadeRunsServer(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	harness := commandHarness{
		run: func(ctx context.Context, name string, arguments ...string) error {
			assert.Equal(t, "lemonade", name)
			assert.Equal(t, []string{"server", "-allow", "127.0.0.1"}, arguments)
			close(started)
			<-release
			return ctx.Err()
		},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := harness.startLemonade(ctx)
	<-started
	cancel()
	close(release)
	<-done
}

func TestProbeTunnelFailureDoesNotCancelPrimaryTunnel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	primaryMapping := portMapping{localPort: 8022, workstationPort: 22}
	probeMapping := portMapping{localPort: 50000, workstationPort: 22}
	primaryStarted := make(chan struct{})
	primaryCompleted := make(chan struct{})
	harness := commandHarness{
		run: func(ctx context.Context, _ string, arguments ...string) error {
			if arguments[4] == "--local-host-port=localhost:50000" {
				return errors.New("probe tunnel failed")
			}
			close(primaryStarted)
			<-ctx.Done()
			close(primaryCompleted)
			return ctx.Err()
		},
	}

	primaryResults := make(chan error, 1)
	go func() {
		primaryResults <- harness.tunnel(ctx, primaryMapping)
	}()
	<-primaryStarted
	probeResults, probeDone := harness.startProbeTunnels(ctx, portMappings{probeMapping})
	result := <-probeResults

	require.Error(t, result.err)
	select {
	case <-primaryCompleted:
		t.Fatal("probe tunnel failure cancelled primary tunnel")
	default:
	}
	cancel()
	require.Error(t, <-primaryResults)
	<-probeDone
}

func TestTunnelSupervisorRestartsTunnelGeneration(t *testing.T) {
	started := make(chan uint16, 4)
	rootContext, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	harness := commandHarness{
		noLemonade: true,
		mappings:   portMappings{{localPort: 8080, workstationPort: 80}},
		run: func(ctx context.Context, _ string, arguments ...string) error {
			localPort := uint16(8080)
			if arguments[4] == "--local-host-port=localhost:50000" {
				localPort = 50000
			}
			started <- localPort
			<-ctx.Done()
			return ctx.Err()
		},
	}
	restartRequests := make(chan struct{}, 1)
	tunnelResults, _, restartResults, done := harness.superviseTunnels(
		rootContext,
		portMappings{{localPort: 50000, workstationPort: 80}},
		restartRequests,
	)

	assert.ElementsMatch(t, []uint16{8080, 50000}, []uint16{<-started, <-started})
	restartRequests <- struct{}{}
	assert.ElementsMatch(t, []uint16{8080, 50000}, []uint16{<-started, <-started})
	assert.Equal(t, 1, <-restartResults)
	assert.Empty(t, tunnelResults)
	cancel()
	<-done
}

func TestProbeTCPConnectsToOpenLocalPort(t *testing.T) {
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })

	accepted := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err == nil {
			_, err = connection.Write([]byte("OK"))
			closeError := connection.Close()
			if err == nil {
				err = closeError
			}
		}
		accepted <- err
	}()

	port := listener.Addr().(*net.TCPAddr).Port
	err = probeTCP(t.Context(), portMapping{localPort: uint16(port)})

	require.NoError(t, err)
	require.NoError(t, <-accepted)
}

func TestProbeTCPDisconnectsFromSSHServer(t *testing.T) {
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })

	request := make(chan []byte, 1)
	serverErrors := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err == nil {
			reader := bufio.NewReader(connection)
			var clientIdentification string
			clientIdentification, err = reader.ReadString('\n')
			if err == nil {
				if _, err = connection.Write([]byte("SSH-2.0-test\r\n")); err == nil {
					var packet []byte
					packet, err = io.ReadAll(reader)
					request <- append([]byte(clientIdentification), packet...)
				}
			}
			closeError := connection.Close()
			if err == nil {
				err = closeError
			}
		}
		serverErrors <- err
	}()

	port := listener.Addr().(*net.TCPAddr).Port
	err = probeTCP(t.Context(), portMapping{localPort: uint16(port), workstationPort: sshPort})

	require.NoError(t, err)
	require.NoError(t, <-serverErrors)
	data := <-request
	requestPrefixLength := len(sshClientIdentification)
	require.GreaterOrEqual(t, len(data), requestPrefixLength+9)
	assert.Equal(t, sshClientIdentification, string(data[:requestPrefixLength]))

	packet := data[requestPrefixLength:]
	packetLength := int(binary.BigEndian.Uint32(packet[:4]))
	assert.Equal(t, len(packet)-4, packetLength)
	assert.Equal(t, byte(sshMsgDisconnect), packet[5])
}

func TestProbeTCPDisconnectsFromEternalTerminalServer(t *testing.T) {
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })

	request := make(chan []byte, 1)
	serverErrors := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err == nil {
			var lengthBytes [8]byte
			_, err = io.ReadFull(connection, lengthBytes[:])
			if err == nil {
				length := binary.NativeEndian.Uint64(lengthBytes[:])
				payload := make([]byte, int(length))
				_, err = io.ReadFull(connection, payload)
				if err == nil {
					request <- append(lengthBytes[:], payload...)
					response := []byte{0x08, eternalTerminalStatusInvalidKey}
					frame := make([]byte, 8+len(response))
					binary.NativeEndian.PutUint64(frame[:8], uint64(len(response)))
					copy(frame[8:], response)
					_, err = connection.Write(frame)
				}
			}
			closeError := connection.Close()
			if err == nil {
				err = closeError
			}
		}
		serverErrors <- err
	}()

	port := listener.Addr().(*net.TCPAddr).Port
	err = probeTCP(t.Context(), portMapping{localPort: uint16(port), workstationPort: eternalTerminalPort})

	require.NoError(t, err)
	require.NoError(t, <-serverErrors)
	data := <-request
	assert.Equal(t, uint64(len(data)-8), binary.NativeEndian.Uint64(data[:8]))
	assert.Equal(t, []byte{0x10, eternalTerminalProtocolVersion}, data[8:])
}

func TestProbeTCPAcceptsSilentOpenConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })

	accepted := make(chan struct{})
	finished := make(chan struct{})
	serverErrors := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err == nil {
			close(accepted)
			<-finished
			err = connection.Close()
		}
		serverErrors <- err
	}()

	port := listener.Addr().(*net.TCPAddr).Port
	err = probeTCPWithTimeout(t.Context(), portMapping{localPort: uint16(port)}, 10*time.Millisecond)

	<-accepted
	close(finished)
	require.NoError(t, err)
	require.NoError(t, <-serverErrors)
}

func TestProbeTCPRejectsImmediatelyClosedConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })

	accepted := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err == nil {
			err = connection.Close()
		}
		accepted <- err
	}()

	port := listener.Addr().(*net.TCPAddr).Port
	err = probeTCPWithTimeout(t.Context(), portMapping{localPort: uint16(port)}, time.Second)

	require.Error(t, err)
	require.NoError(t, <-accepted)
}

func TestProbeTCPRejectsClosedLocalPort(t *testing.T) {
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())

	err = probeTCP(t.Context(), portMapping{localPort: uint16(port)})

	require.Error(t, err)
}

func newTestDashboard(t *testing.T, mappings portMappings, probe portProbe) dashboardModel {
	t.Helper()
	return newDashboard(dashboardConfig{
		ctx:           t.Context(),
		mappings:      mappings,
		probeMappings: mappings,
		probe:         probe,
	})
}

func testProbeResults(results ...probeResult) probeResultsMsg {
	return testProbeResultsForGeneration(0, results...)
}

func testProbeResultsForGeneration(generation int, results ...probeResult) probeResultsMsg {
	return probeResultsMsg{generation: generation, results: results}
}
