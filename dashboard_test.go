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
	model := newDashboard(t.Context(), portMappings{{localPort: 8080, workstationPort: 80}}, nil, probeTCP)

	assert.Equal(t, []mappingStatus{{
		mapping: portMapping{localPort: 8080, workstationPort: 80},
		status:  statusStarting,
	}}, model.mappings)
}

func TestDashboardUpdatesProbeStatuses(t *testing.T) {
	model := newDashboard(t.Context(), portMappings{
		{localPort: 8080, workstationPort: 80},
		{localPort: 5432, workstationPort: 5432},
	}, nil, probeTCP)

	updated, _ := model.Update(probeResultsMsg{
		{index: 0},
		{index: 1, err: errors.New("connection refused")},
	})

	assert.Equal(t, []mappingStatus{
		{mapping: portMapping{localPort: 8080, workstationPort: 80}, status: statusOpen},
		{mapping: portMapping{localPort: 5432, workstationPort: 5432}, status: statusWaiting},
	}, updated.(dashboardModel).mappings)
}

func TestDashboardSchedulesNextProbeAfterResults(t *testing.T) {
	model := newDashboard(t.Context(), portMappings{{localPort: 8080, workstationPort: 80}}, nil, probeTCP)

	_, command := model.Update(probeResultsMsg{{index: 0}})

	assert.NotNil(t, command)
}

func TestDashboardRunsProbeAfterTick(t *testing.T) {
	model := newDashboard(
		t.Context(),
		portMappings{{localPort: 8080, workstationPort: 80}},
		nil,
		func(context.Context, portMapping) error { return nil },
	)

	_, command := model.Update(probeTickMsg{})

	assert.IsType(t, probeResultsMsg{}, command())
}

func TestDashboardStopsMappingsAfterTunnelExit(t *testing.T) {
	model := newDashboard(t.Context(), portMappings{{localPort: 8080, workstationPort: 80}}, nil, probeTCP)

	updated, command := model.Update(tunnelsStoppedMsg{err: errors.New("connection failed")})

	assert.NotNil(t, command)
	assert.Equal(t, statusStopped, updated.(dashboardModel).mappings[0].status)
	assert.ErrorContains(t, updated.(dashboardModel).failure, "connection failed")
}

func TestDashboardViewIncludesPortMappingAndStatus(t *testing.T) {
	model := newDashboard(t.Context(), portMappings{{localPort: 8080, workstationPort: 80}}, nil, probeTCP)
	model.mappings[0].status = statusOpen

	view := model.View().Content

	assert.True(t, strings.Contains(view, "localhost:8080"))
	assert.True(t, strings.Contains(view, "workstation:80"))
	assert.True(t, strings.Contains(view, "OPEN"))
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
