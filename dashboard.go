package main

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
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

const (
	probeInterval             = 2 * time.Second
	probeTimeout              = time.Second
	failedProbesBeforeRestart = 3
	maxProbeInterval          = 30 * time.Second
	probeJitter               = 250 * time.Millisecond
	restartCooldown           = time.Second
)

type tunnelStatus string

const (
	statusStarting tunnelStatus = "STARTING"
	statusOpen     tunnelStatus = "OPEN"
	statusWaiting  tunnelStatus = "WAITING"
	statusUnknown  tunnelStatus = "UNKNOWN"
	statusStopped  tunnelStatus = "STOPPED"
)

type mappingStatus struct {
	mapping                  portMapping
	probe                    portMapping
	status                   tunnelStatus
	consecutiveProbeFailures int
	hasBeenOpen              bool
}

type portProbe func(context.Context, portMapping) error

type probeResult struct {
	index int
	err   error
}

type probeResultsMsg struct {
	generation int
	results    []probeResult
}

type probeTickMsg struct{}

type tunnelsStoppedMsg struct {
	err error
}

type tunnelsRestartedMsg struct {
	generation int
}

type probeTunnelResult struct {
	index      int
	generation int
	err        error
}

type probeTunnelStoppedMsg struct {
	index      int
	generation int
	err        error
}

type dashboardConfig struct {
	ctx                context.Context
	mappings           portMappings
	probeMappings      portMappings
	tunnelResults      <-chan error
	probeTunnelResults <-chan probeTunnelResult
	restartRequests    chan<- struct{}
	restartResults     <-chan int
	probe              portProbe
}

type dashboardModel struct {
	ctx                context.Context
	mappings           []mappingStatus
	probe              portProbe
	tunnelResults      <-chan error
	probeTunnelResults <-chan probeTunnelResult
	restartRequests    chan<- struct{}
	restartResults     <-chan int
	failure            error
	probeFailure       error
	stopped            bool
	restartPending     bool
	generation         int
	probeBackoff       int
}

type tunnelSupervisor struct {
	harness            commandHarness
	ctx                context.Context
	probeMappings      portMappings
	restartRequests    <-chan struct{}
	restartResults     chan<- int
	tunnelResults      chan<- error
	probeTunnelResults chan<- probeTunnelResult
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
		restartRequests:    config.restartRequests,
		restartResults:     config.restartResults,
	}
}

func (model dashboardModel) Init() tea.Cmd {
	return tea.Batch(
		model.probeMappings(),
		model.waitForTunnels(),
		model.waitForProbeTunnels(),
		model.waitForTunnelRestart(),
	)
}

func (model dashboardModel) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	switch message := message.(type) {
	case tea.KeyPressMsg:
		if message.String() == "ctrl+c" || message.String() == "q" {
			return model, tea.Quit
		}
	case probeResultsMsg:
		model.updateStatuses(message)
		return model, model.nextProbe()
	case probeTickMsg:
		return model, model.probeMappings()
	case tunnelsStoppedMsg:
		model.failure = message.err
		model.stopped = true
		for index := range model.mappings {
			model.mappings[index].status = statusStopped
		}
		return model, tea.Quit
	case tunnelsRestartedMsg:
		model.restartPending = false
		model.generation = message.generation
		return model, model.waitForTunnelRestart()
	case probeTunnelStoppedMsg:
		if message.generation != model.generation {
			return model, model.waitForProbeTunnels()
		}
		if model.restartPending {
			return model, model.waitForProbeTunnels()
		}
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
	generation := model.generation
	return func() tea.Msg {
		results := make([]probeResult, len(mappings))
		var group errgroup.Group
		for index, mapping := range mappings {
			group.Go(func() error {
				results[index] = probeResult{index: index, err: model.probe(model.ctx, mapping.probe)}
				return nil
			})
		}
		_ = group.Wait()
		return probeResultsMsg{generation: generation, results: results}
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
		return probeTunnelStoppedMsg{index: result.index, generation: result.generation, err: result.err}
	}
}

func (model *dashboardModel) updateStatuses(results probeResultsMsg) {
	if model.restartPending || results.generation != model.generation {
		return
	}
	maxProbeFailures := 0
	for _, result := range results.results {
		mapping := &model.mappings[result.index]
		if mapping.status == statusUnknown {
			continue
		}
		if result.err == nil {
			mapping.status = statusOpen
			mapping.hasBeenOpen = true
			mapping.consecutiveProbeFailures = 0
			continue
		}
		mapping.status = statusWaiting
		mapping.consecutiveProbeFailures++
		if mapping.consecutiveProbeFailures > maxProbeFailures {
			maxProbeFailures = mapping.consecutiveProbeFailures
		}
		if mapping.hasBeenOpen && mapping.consecutiveProbeFailures >= failedProbesBeforeRestart {
			model.requestRestart()
			return
		}
	}
	model.probeBackoff = maxProbeFailures
}

func (model *dashboardModel) requestRestart() {
	if model.restartPending {
		return
	}
	model.restartPending = true
	for index := range model.mappings {
		model.mappings[index].status = statusStarting
		model.mappings[index].consecutiveProbeFailures = 0
		model.mappings[index].hasBeenOpen = false
	}
	model.probeFailure = nil
	model.probeBackoff = 0
	select {
	case model.restartRequests <- struct{}{}:
	default:
	}
}

func (model dashboardModel) waitForTunnelRestart() tea.Cmd {
	return func() tea.Msg {
		generation, ok := <-model.restartResults
		if !ok {
			return nil
		}
		return tunnelsRestartedMsg{generation: generation}
	}
}

func (model *dashboardModel) recordProbeFailure(message probeTunnelStoppedMsg) {
	if message.err == nil || errors.Is(message.err, context.Canceled) || model.probeFailure != nil {
		return
	}
	model.probeFailure = fmt.Errorf("probe tunnel %s: %w", model.mappings[message.index].mapping, message.err)
}

func (model dashboardModel) nextProbe() tea.Cmd {
	delay := probeInterval
	for range model.probeBackoff {
		delay *= 2
		if delay >= maxProbeInterval {
			delay = maxProbeInterval
			break
		}
	}
	jitter := time.Duration(rand.Int63n(int64(2*probeJitter)+1)) - probeJitter
	delay += jitter
	if delay > maxProbeInterval {
		delay = maxProbeInterval
	}
	return tea.Tick(delay, func(time.Time) tea.Msg {
		return probeTickMsg{}
	})
}

func probeTCP(ctx context.Context, mapping portMapping) error {
	return probeTCPWithTimeout(ctx, mapping, probeTimeout)
}

const (
	sshPort                    = 22
	sshClientIdentification    = "SSH-2.0-gcloud-tunnel-probe\r\n"
	sshIdentificationPrefix    = "SSH-"
	sshMaxIdentificationSize   = 255
	sshMsgDisconnect           = 1
	sshDisconnectByApplication = 11

	eternalTerminalPort             = 2022
	eternalTerminalProtocolVersion  = 6
	eternalTerminalStatusInvalidKey = 3
	eternalTerminalStatusMismatched = 4
	eternalTerminalMaxResponseSize  = 4 * 1024
)

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

	switch mapping.workstationPort {
	case sshPort:
		if err := writeSSHClientIdentification(connection, deadline); err != nil {
			return err
		}
		return probeServerResponse(connection, deadline)
	case eternalTerminalPort:
		return probeEternalTerminal(connection, deadline)
	default:
		return probeServerResponse(connection, deadline)
	}
}

func probeServerResponse(connection net.Conn, deadline time.Time) error {
	if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}

	var firstByte [1]byte
	count, err := connection.Read(firstByte[:])
	if count == 0 {
		if networkError, ok := errors.AsType[net.Error](err); ok && networkError.Timeout() {
			return nil
		}
		return err
	}
	if firstByte[0] != sshIdentificationPrefix[0] {
		return nil
	}

	var remainingPrefix [len(sshIdentificationPrefix) - 1]byte
	count, err = io.ReadFull(connection, remainingPrefix[:])
	if count < len(remainingPrefix) {
		return nil
	}
	if err != nil || string(append([]byte{firstByte[0]}, remainingPrefix[:]...)) != sshIdentificationPrefix {
		return nil
	}

	identification := append([]byte(sshIdentificationPrefix), remainingPrefix[:]...)
	for len(identification) < sshMaxIdentificationSize {
		var nextByte [1]byte
		count, err := connection.Read(nextByte[:])
		if count > 0 {
			identification = append(identification, nextByte[0])
			if nextByte[0] == '\n' {
				break
			}
		}
		if err != nil {
			if networkError, ok := errors.AsType[net.Error](err); ok && networkError.Timeout() {
				return nil
			}
			return err
		}
	}
	if identification[len(identification)-1] != '\n' {
		return nil
	}

	return sendSSHProbeDisconnect(connection, deadline)
}

func writeProbeBytes(connection net.Conn, data []byte, deadline time.Time) error {
	if err := connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	for len(data) > 0 {
		count, err := connection.Write(data)
		if count > 0 {
			data = data[count:]
		}
		if err != nil {
			return err
		}
		if count == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func writeSSHClientIdentification(connection net.Conn, deadline time.Time) error {
	return writeProbeBytes(connection, []byte(sshClientIdentification), deadline)
}

func sendSSHProbeDisconnect(connection net.Conn, deadline time.Time) error {
	description := []byte("gcloud-tunnel probe")
	payloadLength := 1 + 4 + 4 + len(description) + 4
	paddingLength := 8 - ((1 + payloadLength) % 8)
	if paddingLength < 4 {
		paddingLength += 8
	}
	packetLength := 1 + payloadLength + paddingLength
	packet := make([]byte, 4+packetLength)
	binary.BigEndian.PutUint32(packet[:4], uint32(packetLength))
	packet[4] = byte(paddingLength)

	offset := 5
	packet[offset] = sshMsgDisconnect
	offset++
	binary.BigEndian.PutUint32(packet[offset:offset+4], sshDisconnectByApplication)
	offset += 4
	binary.BigEndian.PutUint32(packet[offset:offset+4], uint32(len(description)))
	offset += 4
	offset += copy(packet[offset:], description)
	binary.BigEndian.PutUint32(packet[offset:offset+4], 0)
	offset += 4
	if _, err := cryptorand.Read(packet[offset:]); err != nil {
		return err
	}

	return writeProbeBytes(connection, packet, deadline)
}

func probeEternalTerminal(connection net.Conn, deadline time.Time) error {
	// ET frames protobuf messages with a native-endian int64 length. An empty
	// client ID is guaranteed not to match a real terminal session; using the
	// current protocol version makes the server return INVALID_KEY instead of
	// logging a protocol mismatch.
	request := []byte{0x10, eternalTerminalProtocolVersion}
	frame := make([]byte, 8+len(request))
	binary.NativeEndian.PutUint64(frame[:8], uint64(len(request)))
	copy(frame[8:], request)
	if err := writeProbeBytes(connection, frame, deadline); err != nil {
		return err
	}

	if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	var responseLengthBytes [8]byte
	if _, err := io.ReadFull(connection, responseLengthBytes[:]); err != nil {
		if networkError, ok := errors.AsType[net.Error](err); ok && networkError.Timeout() {
			return nil
		}
		return err
	}
	responseLength := binary.NativeEndian.Uint64(responseLengthBytes[:])
	if responseLength > eternalTerminalMaxResponseSize {
		return fmt.Errorf("Eternal Terminal response is too large: %d bytes", responseLength)
	}
	response := make([]byte, int(responseLength))
	if _, err := io.ReadFull(connection, response); err != nil {
		if networkError, ok := errors.AsType[net.Error](err); ok && networkError.Timeout() {
			return nil
		}
		return err
	}
	status, ok := eternalTerminalResponseStatus(response)
	if !ok {
		return errors.New("invalid Eternal Terminal response")
	}
	if status != eternalTerminalStatusInvalidKey && status != eternalTerminalStatusMismatched {
		return fmt.Errorf("unexpected Eternal Terminal response status: %d", status)
	}
	return nil
}

func eternalTerminalResponseStatus(response []byte) (uint64, bool) {
	for offset := 0; offset < len(response); {
		tag, count, ok := readProtoVarint(response[offset:])
		if !ok || tag == 0 {
			return 0, false
		}
		offset += count
		fieldNumber := tag >> 3
		wireType := tag & 7
		switch wireType {
		case 0:
			value, count, ok := readProtoVarint(response[offset:])
			if !ok {
				return 0, false
			}
			offset += count
			if fieldNumber == 1 {
				return value, true
			}
		case 1:
			if len(response)-offset < 8 {
				return 0, false
			}
			offset += 8
		case 2:
			length, count, ok := readProtoVarint(response[offset:])
			if !ok {
				return 0, false
			}
			offset += count
			if length > uint64(len(response)-offset) {
				return 0, false
			}
			offset += int(length)
		case 5:
			if len(response)-offset < 4 {
				return 0, false
			}
			offset += 4
		default:
			return 0, false
		}
	}
	return 0, false
}

func readProtoVarint(data []byte) (uint64, int, bool) {
	var value uint64
	for index, byteValue := range data {
		if index == 9 && byteValue > 1 {
			return 0, 0, false
		}
		if index >= 10 {
			return 0, 0, false
		}
		value |= uint64(byteValue&0x7f) << (7 * index)
		if byteValue < 0x80 {
			return value, index + 1, true
		}
	}
	return 0, 0, false
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

	restartRequests := make(chan struct{}, 1)
	tunnelResults, probeTunnelResults, restartResults, tunnelsDone := harness.superviseTunnels(
		tunnelContext,
		probeMappings,
		restartRequests,
	)

	program := tea.NewProgram(
		newDashboard(dashboardConfig{
			ctx:                tunnelContext,
			mappings:           harness.mappings,
			probeMappings:      probeMappings,
			tunnelResults:      tunnelResults,
			probeTunnelResults: probeTunnelResults,
			restartRequests:    restartRequests,
			restartResults:     restartResults,
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
	if err != nil {
		return err
	}
	failure := model.(dashboardModel).failure
	if failure == nil {
		return nil
	}
	return dashboardError{err: failure}
}

func (harness commandHarness) superviseTunnels(
	ctx context.Context,
	probeMappings portMappings,
	restartRequests <-chan struct{},
) (<-chan error, <-chan probeTunnelResult, <-chan int, <-chan struct{}) {
	tunnelResults := make(chan error, 1)
	probeTunnelResults := make(chan probeTunnelResult, len(probeMappings))
	restartResults := make(chan int, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(probeTunnelResults)
		defer close(restartResults)
		supervisor := tunnelSupervisor{
			harness:            harness,
			ctx:                ctx,
			probeMappings:      probeMappings,
			restartRequests:    restartRequests,
			restartResults:     restartResults,
			tunnelResults:      tunnelResults,
			probeTunnelResults: probeTunnelResults,
		}
		supervisor.run()
	}()
	return tunnelResults, probeTunnelResults, restartResults, done
}

func (supervisor tunnelSupervisor) run() {
	for generation := 0; supervisor.ctx.Err() == nil && supervisor.superviseGeneration(generation); generation++ {
	}
}

func (supervisor tunnelSupervisor) superviseGeneration(generation int) bool {
	generationContext, cancel := context.WithCancel(supervisor.ctx)
	defer cancel()
	primaryResults, primaryDone := supervisor.harness.startTunnelGeneration(generationContext)
	probeResults, probesDone := supervisor.harness.startProbeTunnels(generationContext, supervisor.probeMappings)
	for primaryResults != nil || probeResults != nil {
		select {
		case err := <-primaryResults:
			if supervisor.ctx.Err() != nil {
				<-probesDone
				return false
			}
			cancel()
			supervisor.tunnelResults <- err
			<-primaryDone
			<-probesDone
			return false
		case result, ok := <-probeResults:
			if !ok {
				probeResults = nil
				continue
			}
			result.generation = generation
			supervisor.probeTunnelResults <- result
		case <-supervisor.restartRequests:
			cancel()
			<-primaryDone
			<-probesDone
			if supervisor.ctx.Err() != nil {
				return false
			}
			timer := time.NewTimer(restartCooldown)
			select {
			case <-timer.C:
			case <-supervisor.ctx.Done():
				timer.Stop()
				return false
			}
			supervisor.restartResults <- generation + 1
			return true
		case <-supervisor.ctx.Done():
			<-primaryDone
			<-probesDone
			return false
		}
	}
	<-primaryDone
	return false
}

func (harness commandHarness) startTunnelGeneration(ctx context.Context) (<-chan error, <-chan struct{}) {
	results := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		results <- harness.startTunnels(ctx)
		close(done)
	}()
	return results, done
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
