package main

import (
	"context"
	"fmt"
	"image/color"
	"slices"
	"strconv"

	"charm.land/fang/v2"
	"charm.land/lipgloss/v2"
	"golang.org/x/sync/errgroup"
)

const lemonadePort uint16 = 2489

type commandHarness struct {
	account          string
	project          string
	cluster          string
	config           string
	region           string
	startWorkstation bool
	noLemonade       bool
	workstation      string

	mappings portMappings

	run func(ctx context.Context, name string, arg ...string) error
}

func (harness commandHarness) validatePortMappings() error {
	mappings := harness.tunnelMappings()
	localPorts := make(set[uint16], len(mappings))
	workstationPorts := make(set[uint16], len(mappings))
	for _, mapping := range mappings {
		if !localPorts.add(mapping.localPort) {
			return fmt.Errorf("local port %d is published more than once", mapping.localPort)
		}
		if !workstationPorts.add(mapping.workstationPort) {
			return fmt.Errorf("workstation port %d is published more than once", mapping.workstationPort)
		}
	}
	return nil
}

func (harness commandHarness) tunnelMappings() portMappings {
	if harness.noLemonade || slices.Contains(harness.mappings, lemonadeMapping()) {
		return harness.mappings
	}
	return append(slices.Clone(harness.mappings), lemonadeMapping())
}

func (harness commandHarness) monitoredMappings() portMappings {
	if harness.noLemonade {
		return harness.mappings
	}
	mappings := make(portMappings, 0, len(harness.mappings))
	for _, mapping := range harness.mappings {
		if mapping.localPort == lemonadePort || mapping.workstationPort == lemonadePort {
			continue
		}
		mappings = append(mappings, mapping)
	}
	return mappings
}

func lemonadeMapping() portMapping {
	return portMapping{localPort: lemonadePort, workstationPort: lemonadePort}
}

func (harness commandHarness) startTunnels(ctx context.Context) error {
	if err := harness.validatePortMappings(); err != nil {
		return err
	}
	group, groupContext := errgroup.WithContext(ctx)
	for _, mapping := range harness.tunnelMappings() {
		group.Go(func() error {
			if err := harness.tunnel(groupContext, mapping); err != nil {
				return fmt.Errorf("tunnel %s: %w", mapping, err)
			}
			return nil
		})
	}
	return group.Wait()
}

func (harness commandHarness) startLemonadeServer(ctx context.Context) error {
	return harness.run(ctx, "lemonade", "server", "-allow", "127.0.0.1")
}

func (harness commandHarness) tunnel(ctx context.Context, mapping portMapping) error {
	return harness.run(ctx, "gcloud", harness.gcloudArguments(mapping)...)
}

func (harness commandHarness) gcloudArguments(mapping portMapping) []string {
	arguments := []string{
		"workstations",
		"start-tcp-tunnel",
		harness.workstation,
		strconv.FormatUint(uint64(mapping.workstationPort), 10),
		"--local-host-port=localhost:" + strconv.FormatUint(uint64(mapping.localPort), 10),
		"--cluster=" + harness.cluster,
		"--config=" + harness.config,
		"--region=" + harness.region,
	}
	if harness.startWorkstation {
		arguments = append(arguments, "--start-workstation")
	}
	if harness.project != "" {
		arguments = append(arguments, "--project="+harness.project)
	}
	if harness.account != "" {
		arguments = append(arguments, "--account="+harness.account)
	}
	return arguments
}

func tunnelColorScheme(lightDark lipgloss.LightDarkFunc) fang.ColorScheme {
	return fang.ColorScheme{
		Base:           lightDark(lipgloss.Black, lipgloss.White),
		Title:          lipgloss.Color("#5B21B6"),
		Description:    lightDark(lipgloss.Black, lipgloss.White),
		Codeblock:      lightDark(lipgloss.Color("#EDE9FE"), lipgloss.Color("#1E1B4B")),
		Program:        lipgloss.Color("#2563EB"),
		DimmedArgument: lightDark(lipgloss.BrightBlack, lipgloss.BrightBlack),
		Comment:        lightDark(lipgloss.BrightBlack, lipgloss.BrightBlack),
		Flag:           lipgloss.Color("#059669"),
		FlagDefault:    lipgloss.Color("#7C3AED"),
		Command:        lipgloss.Color("#EA580C"),
		QuotedString:   lipgloss.Color("#DB2777"),
		Argument:       lightDark(lipgloss.Black, lipgloss.White),
		Help:           lightDark(lipgloss.Black, lipgloss.White),
		Dash:           lightDark(lipgloss.Black, lipgloss.White),
		ErrorHeader:    [2]color.Color{lipgloss.White, lipgloss.Red},
		ErrorDetails:   lipgloss.Red,
	}
}
