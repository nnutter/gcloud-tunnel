package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"golang.org/x/sync/errgroup"
)

const probeInterval = 2 * time.Second
const probeTimeout = time.Second

type tunnelStatus string

const (
	statusStarting tunnelStatus = "STARTING"
	statusOpen     tunnelStatus = "OPEN"
	statusWaiting  tunnelStatus = "WAITING"
	statusUnknown  tunnelStatus = "UNKNOWN"
	statusStopped  tunnelStatus = "STOPPED"
)

type mappingStatus struct {
	mapping portMapping
	probe   portMapping
	status  tunnelStatus
}

type portProbe func(context.Context, portMapping) error

type probeResult struct {
	index int
	err   error
}

type probeResultsMsg []probeResult

type probeTickMsg struct{}

type tunnelsStoppedMsg struct {
	err error
}

type probeTunnelResult struct {
	index int
	err   error
}

type probeTunnelStoppedMsg struct {
	index int
	err   error
}

type dashboardConfig struct {
	ctx                context.Context
	mappings           portMappings
	probeMappings      portMappings
	tunnelResults      <-chan error
	probeTunnelResults <-chan probeTunnelResult
	probe              portProbe
}

type dashboardModel struct {
	ctx                context.Context
	mappings           []mappingStatus
	probe              portProbe
	tunnelResults      <-chan error
	probeTunnelResults <-chan probeTunnelResult
	failure            error
	probeFailure       error
	stopped            bool
}

func newDashboard(config dashboardConfig) dashboardModel {
	statuses := make([]mappingStatus, len(config.mappings))
	for index, mapping := range config.mappings {
		statuses[index] = mappingStatus{
			mapping: mapping,
			probe:   config.probeMappings[index],
			status:  statusStarting,
		}
	}
	return dashboardModel{
		ctx:                config.ctx,
		mappings:           statuses,
		probe:              config.probe,
		tunnelResults:      config.tunnelResults,
		probeTunnelResults: config.probeTunnelResults,
	}
}

func (model dashboardModel) Init() tea.Cmd {
	return tea.Batch(model.probeMappings(), model.waitForTunnels(), model.waitForProbeTunnels())
}

func (model dashboardModel) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	switch message := message.(type) {
	case tea.KeyPressMsg:
		if message.String() == "ctrl+c" || message.String() == "q" {
			return model, tea.Quit
		}
	case probeResultsMsg:
		model.updateStatuses(message)
		return model, nextProbe()
	case probeTickMsg:
		return model, model.probeMappings()
	case tunnelsStoppedMsg:
		model.failure = message.err
		model.stopped = true
		for index := range model.mappings {
			model.mappings[index].status = statusStopped
		}
		return model, tea.Quit
	case probeTunnelStoppedMsg:
		model.mappings[message.index].status = statusUnknown
		model.recordProbeFailure(message)
		return model, model.waitForProbeTunnels()
	}
	return model, nil
}

func (model dashboardModel) View() tea.View {
	var lines []string
	lines = append(lines, titleStyle.Render("Cloud Workstation Tunnels"))
	lines = append(lines, "")
	for _, mapping := range model.mappings {
		line := fmt.Sprintf(
			"  %s  localhost:%-5d -> workstation:%-5d",
			statusStyle(mapping.status).Render(string(mapping.status)),
			mapping.mapping.localPort,
			mapping.mapping.workstationPort,
		)
		lines = append(lines, line)
	}
	lines = append(lines, "")
	if model.failure != nil {
		lines = append(lines, errorStyle.Render(model.failure.Error()))
	} else if model.stopped {
		lines = append(lines, dimStyle.Render("Tunnels stopped."))
	} else if model.probeFailure != nil {
		lines = append(lines, waitStyle.Render(model.probeFailure.Error()))
	} else {
		lines = append(lines, dimStyle.Render("TCP checks every 2s. Press q to stop."))
	}
	return tea.NewView(strings.Join(lines, "\n") + "\n")
}

func (model dashboardModel) probeMappings() tea.Cmd {
	mappings := slices.Clone(model.mappings)
	return func() tea.Msg {
		results := make(probeResultsMsg, len(mappings))
		var group errgroup.Group
		for index, mapping := range mappings {
			group.Go(func() error {
				results[index] = probeResult{index: index, err: model.probe(model.ctx, mapping.probe)}
				return nil
			})
		}
		_ = group.Wait()
		return results
	}
}

func (model dashboardModel) waitForTunnels() tea.Cmd {
	return func() tea.Msg {
		return tunnelsStoppedMsg{err: <-model.tunnelResults}
	}
}

func (model dashboardModel) waitForProbeTunnels() tea.Cmd {
	return func() tea.Msg {
		result, ok := <-model.probeTunnelResults
		if !ok {
			return nil
		}
		return probeTunnelStoppedMsg{index: result.index, err: result.err}
	}
}

func (model *dashboardModel) updateStatuses(results probeResultsMsg) {
	for _, result := range results {
		if model.mappings[result.index].status == statusUnknown {
			continue
		}
		if result.err == nil {
			model.mappings[result.index].status = statusOpen
			continue
		}
		model.mappings[result.index].status = statusWaiting
	}
}

func (model *dashboardModel) recordProbeFailure(message probeTunnelStoppedMsg) {
	if message.err == nil || errors.Is(message.err, context.Canceled) || model.probeFailure != nil {
		return
	}
	model.probeFailure = fmt.Errorf("probe tunnel %s: %w", model.mappings[message.index].mapping, message.err)
}

func nextProbe() tea.Cmd {
	return tea.Tick(probeInterval, func(time.Time) tea.Msg {
		return probeTickMsg{}
	})
}

func probeTCP(ctx context.Context, mapping portMapping) error {
	return probeTCPWithTimeout(ctx, mapping, probeTimeout)
}

func probeTCPWithTimeout(ctx context.Context, mapping portMapping, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	dialer := new(net.Dialer)
	connection, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort("localhost", strconv.Itoa(int(mapping.localPort))))
	if err != nil {
		return err
	}
	defer connection.Close()
	if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}

	var response [1]byte
	count, err := connection.Read(response[:])
	if count > 0 {
		return nil
	}
	if networkError, ok := errors.AsType[net.Error](err); ok && networkError.Timeout() {
		return nil
	}
	return err
}

func (harness commandHarness) runDashboard(
	ctx context.Context,
	input io.Reader,
	output io.Writer,
) error {
	tunnelContext, cancel := context.WithCancel(ctx)
	defer cancel()
	probeMappings, err := harness.allocateProbeMappings()
	if err != nil {
		return err
	}

	tunnelResults := make(chan error, 1)
	tunnelsDone := make(chan struct{})
	go func() {
		tunnelResults <- harness.startTunnels(tunnelContext)
		close(tunnelsDone)
	}()
	probeTunnelResults, probeTunnelsDone := harness.startProbeTunnels(tunnelContext, probeMappings)

	program := tea.NewProgram(
		newDashboard(dashboardConfig{
			ctx:                tunnelContext,
			mappings:           harness.mappings,
			probeMappings:      probeMappings,
			tunnelResults:      tunnelResults,
			probeTunnelResults: probeTunnelResults,
			probe:              probeTCP,
		}),
		tea.WithContext(tunnelContext),
		tea.WithInput(input),
		tea.WithOutput(output),
		tea.WithoutSignalHandler(),
	)
	model, err := program.Run()
	cancel()
	<-tunnelsDone
	<-probeTunnelsDone
	if err != nil {
		return err
	}
	failure := model.(dashboardModel).failure
	if failure == nil {
		return nil
	}
	return dashboardError{err: failure}
}

func (harness commandHarness) allocateProbeMappings() (portMappings, error) {
	occupiedPorts := make(set[uint16], len(harness.mappings)*2)
	for _, mapping := range harness.mappings {
		occupiedPorts.add(mapping.localPort)
	}

	probeMappings := make(portMappings, len(harness.mappings))
	for index, mapping := range harness.mappings {
		port, err := allocateProbePort(occupiedPorts)
		if err != nil {
			return nil, err
		}
		probeMappings[index] = portMapping{localPort: port, workstationPort: mapping.workstationPort}
	}
	return probeMappings, nil
}

func allocateProbePort(occupiedPorts set[uint16]) (uint16, error) {
	for {
		listener, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			return 0, err
		}
		port := uint16(listener.Addr().(*net.TCPAddr).Port)
		if err := listener.Close(); err != nil {
			return 0, err
		}
		if occupiedPorts.add(port) {
			return port, nil
		}
	}
}

func (harness commandHarness) startProbeTunnels(
	ctx context.Context,
	mappings portMappings,
) (<-chan probeTunnelResult, <-chan struct{}) {
	results := make(chan probeTunnelResult, len(mappings))
	done := make(chan struct{})
	var group sync.WaitGroup
	for index, mapping := range mappings {
		group.Go(func() {
			results <- probeTunnelResult{index: index, err: harness.tunnel(ctx, mapping)}
		})
	}
	go func() {
		group.Wait()
		close(results)
		close(done)
	}()
	return results, done
}

var (
	titleStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#3B82F6"))
	dimStyle   = lipgloss.NewStyle().Foreground(lipgloss.BrightBlack)
	errorStyle = lipgloss.NewStyle().Foreground(lipgloss.Red)
	openStyle  = lipgloss.NewStyle().Foreground(lipgloss.Green)
	waitStyle  = lipgloss.NewStyle().Foreground(lipgloss.Yellow)
	stopStyle  = lipgloss.NewStyle().Foreground(lipgloss.BrightBlack)
)

func statusStyle(status tunnelStatus) lipgloss.Style {
	switch status {
	case statusOpen:
		return openStyle
	case statusStopped:
		return stopStyle
	case statusUnknown:
		return errorStyle
	default:
		return waitStyle
	}
}
