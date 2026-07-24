package main

import (
	"context"
	"errors"
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

	updated, _ := model.Update(probeResultsMsg{
		{index: 0},
		{index: 1, err: errors.New("connection refused")},
	})

	assert.Equal(t, []mappingStatus{
		{
			mapping: portMapping{localPort: 8080, workstationPort: 80},
			probe:   portMapping{localPort: 8080, workstationPort: 80},
			status:  statusOpen,
		},
		{
			mapping: portMapping{localPort: 5432, workstationPort: 5432},
			probe:   portMapping{localPort: 5432, workstationPort: 5432},
			status:  statusWaiting,
		},
	}, updated.(dashboardModel).mappings)
}

func TestDashboardSchedulesNextProbeAfterResults(t *testing.T) {
	model := newTestDashboard(t, portMappings{{localPort: 8080, workstationPort: 80}}, probeTCP)

	_, command := model.Update(probeResultsMsg{{index: 0}})

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

	updated, _ := model.Update(probeResultsMsg{{index: 0}})

	assert.Equal(t, statusUnknown, updated.(dashboardModel).mappings[0].status)
}

func TestCommandHarnessAllocatesHiddenProbeMappings(t *testing.T) {
	harness := commandHarness{mappings: portMappings{
		{localPort: 8022, workstationPort: 22},
		{localPort: 8080, workstationPort: 8080},
	}}

	probeMappings, err := harness.allocateProbeMappings()

	require.NoError(t, err)
	require.Len(t, probeMappings, 2)
	assert.NotEqual(t, uint16(8022), probeMappings[0].localPort)
	assert.NotEqual(t, uint16(8080), probeMappings[1].localPort)
	assert.NotEqual(t, probeMappings[0].localPort, probeMappings[1].localPort)
	assert.Equal(t, uint16(22), probeMappings[0].workstationPort)
	assert.Equal(t, uint16(8080), probeMappings[1].workstationPort)
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

func TestProbeTCPConnectsToOpenLocalPort(t *testing.T) {
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })

	accepted := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err == nil {
			_, err = connection.Write([]byte("SSH-2.0-test\r\n"))
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
