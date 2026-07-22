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
	statusStopped  tunnelStatus = "STOPPED"
)

type mappingStatus struct {
	mapping portMapping
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

type dashboardModel struct {
	ctx           context.Context
	mappings      []mappingStatus
	probe         portProbe
	tunnelResults <-chan error
	failure       error
	stopped       bool
}

func newDashboard(
	ctx context.Context,
	mappings portMappings,
	tunnelResults <-chan error,
	probe portProbe,
) dashboardModel {
	statuses := make([]mappingStatus, len(mappings))
	for index, mapping := range mappings {
		statuses[index] = mappingStatus{mapping: mapping, status: statusStarting}
	}
	return dashboardModel{
		ctx:           ctx,
		mappings:      statuses,
		probe:         probe,
		tunnelResults: tunnelResults,
	}
}

func (model dashboardModel) Init() tea.Cmd {
	return tea.Batch(model.probeMappings(), model.waitForTunnels())
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
				results[index] = probeResult{index: index, err: model.probe(model.ctx, mapping.mapping)}
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

func (model *dashboardModel) updateStatuses(results probeResultsMsg) {
	for _, result := range results {
		if result.err == nil {
			model.mappings[result.index].status = statusOpen
			continue
		}
		model.mappings[result.index].status = statusWaiting
	}
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

	tunnelResults := make(chan error, 1)
	tunnelsDone := make(chan struct{})
	go func() {
		tunnelResults <- harness.startTunnels(tunnelContext)
		close(tunnelsDone)
	}()

	program := tea.NewProgram(
		newDashboard(tunnelContext, harness.mappings, tunnelResults, probeTCP),
		tea.WithContext(tunnelContext),
		tea.WithInput(input),
		tea.WithOutput(output),
		tea.WithoutSignalHandler(),
	)
	model, err := program.Run()
	cancel()
	<-tunnelsDone
	if err != nil {
		return err
	}
	return model.(dashboardModel).failure
}

var (
	titleStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#5B21B6"))
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
	default:
		return waitStyle
	}
}
