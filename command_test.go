package main

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommandStartsTunnelWithForwardedOptions(t *testing.T) {
	var commandName string
	var commandArguments []string
	harness := commandHarness{
		account:          "account@example.com",
		cluster:          "cluster",
		config:           "config",
		project:          "project",
		region:           "us-central1",
		startWorkstation: true,
		workstation:      "workstation",
		run: func(_ context.Context, name string, arguments ...string) error {
			commandName = name
			commandArguments = arguments
			return nil
		},
	}

	require.NoError(t, harness.tunnel(t.Context(), portMapping{localPort: 8080, workstationPort: 80}))
	assert.Equal(t, "gcloud", commandName)
	assert.Equal(t, []string{
		"workstations",
		"start-tcp-tunnel",
		"workstation",
		"80",
		"--local-host-port=localhost:8080",
		"--cluster=cluster",
		"--config=config",
		"--region=us-central1",
		"--start-workstation",
		"--project=project",
		"--account=account@example.com",
	}, commandArguments)
}

func TestCommandRequiresWorkstationOptionsAndPublish(t *testing.T) {
	testCases := []struct {
		name    string
		args    []string
		message string
	}{
		{
			name:    "missing cluster",
			args:    []string{"workstation", "--config", "config", "--region", "region", "-p", "8080:80"},
			message: "required flag(s) \"cluster\" not set",
		},
		{
			name:    "missing config",
			args:    []string{"workstation", "--cluster", "cluster", "--region", "region", "-p", "8080:80"},
			message: "required flag(s) \"config\" not set",
		},
		{
			name:    "missing region",
			args:    []string{"workstation", "--cluster", "cluster", "--config", "config", "-p", "8080:80"},
			message: "required flag(s) \"region\" not set",
		},
		{
			name:    "missing publish",
			args:    []string{"workstation", "--cluster", "cluster", "--config", "config", "--region", "region"},
			message: "required flag(s) \"publish\" not set",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			command := newCommand()
			command.SetArgs(testCase.args)
			command.SetOut(io.Discard)
			command.SetErr(io.Discard)

			err := command.ExecuteContext(t.Context())
			require.Error(t, err)
			assert.ErrorContains(t, err, testCase.message)
		})
	}
}

func TestCommandRejectsDuplicatePortsBeforeStartingTunnels(t *testing.T) {
	testCases := []struct {
		name     string
		mappings []string
		message  string
	}{
		{
			name:     "duplicate local port",
			mappings: []string{"8080:80", "8080:443"},
			message:  "local port 8080 is published more than once",
		},
		{
			name:     "duplicate workstation port",
			mappings: []string{"8080:80", "8443:80"},
			message:  "workstation port 80 is published more than once",
		},
		{
			name:     "identical mapping",
			mappings: []string{"8080:80", "8080:80"},
			message:  "local port 8080 is published more than once",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var calls int
			var mappings portMappings
			for _, mapping := range testCase.mappings {
				require.NoError(t, mappings.Set(mapping))
			}
			harness := commandHarness{
				mappings: mappings,
				run: func(context.Context, string, ...string) error {
					calls++
					return nil
				},
			}

			err := harness.startTunnels(t.Context())
			require.Error(t, err)
			assert.ErrorContains(t, err, testCase.message)
			assert.Zero(t, calls)
		})
	}
}

func TestStartTunnelsRunsMappingsConcurrently(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		harness := commandHarness{
			noLemonade: true,
			mappings: portMappings{
				{localPort: 8080, workstationPort: 80},
				{localPort: 5432, workstationPort: 5432},
			},
			run: func(context.Context, string, ...string) error {
				started <- struct{}{}
				<-release
				return nil
			},
		}
		done <- harness.startTunnels(t.Context())
	}()

	<-started
	<-started
	close(release)

	require.NoError(t, <-done)
}

func TestStartTunnelsCancelsSiblingAfterFailure(t *testing.T) {
	var mutex sync.Mutex
	var siblingCancelled bool

	harness := commandHarness{
		noLemonade: true,
		mappings: portMappings{
			{localPort: 8080, workstationPort: 80},
			{localPort: 5432, workstationPort: 5432},
		},
		run: func(ctx context.Context, _ string, arguments ...string) error {
			if arguments[3] == "80" {
				return errors.New("connection failed")
			}
			<-ctx.Done()
			mutex.Lock()
			siblingCancelled = true
			mutex.Unlock()
			return ctx.Err()
		},
	}
	err := harness.startTunnels(t.Context())

	require.Error(t, err)
	assert.ErrorContains(t, err, "tunnel 8080:80: connection failed")
	mutex.Lock()
	assert.True(t, siblingCancelled)
	mutex.Unlock()
}

func TestTunnelMappingsIncludesLemonadeByDefault(t *testing.T) {
	harness := commandHarness{
		mappings: portMappings{{localPort: 8080, workstationPort: 80}},
	}

	assert.Equal(t, portMappings{
		{localPort: 8080, workstationPort: 80},
		{localPort: lemonadePort, workstationPort: lemonadePort},
	}, harness.tunnelMappings())
}

func TestTunnelMappingsSkipsDuplicateLemonadeMapping(t *testing.T) {
	harness := commandHarness{
		mappings: portMappings{
			{localPort: 8080, workstationPort: 80},
			{localPort: lemonadePort, workstationPort: lemonadePort},
		},
	}

	assert.Equal(t, harness.mappings, harness.tunnelMappings())
}

func TestTunnelMappingsOmitsLemonadeWhenDisabled(t *testing.T) {
	harness := commandHarness{
		noLemonade: true,
		mappings:   portMappings{{localPort: 8080, workstationPort: 80}},
	}

	assert.Equal(t, harness.mappings, harness.tunnelMappings())
}

func TestMonitoredMappingsOmitLemonadePort(t *testing.T) {
	harness := commandHarness{
		mappings: portMappings{
			{localPort: 8080, workstationPort: 80},
			{localPort: lemonadePort, workstationPort: lemonadePort},
		},
	}

	assert.Equal(t, portMappings{
		{localPort: 8080, workstationPort: 80},
	}, harness.monitoredMappings())
}

func TestMonitoredMappingsKeepLemonadeWhenDisabled(t *testing.T) {
	harness := commandHarness{
		noLemonade: true,
		mappings: portMappings{
			{localPort: 8080, workstationPort: 80},
			{localPort: lemonadePort, workstationPort: lemonadePort},
		},
	}

	assert.Equal(t, harness.mappings, harness.monitoredMappings())
}

func TestStartTunnelsIncludesLemonadeMapping(t *testing.T) {
	started := make(chan string, 3)
	release := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		harness := commandHarness{
			mappings: portMappings{{localPort: 8080, workstationPort: 80}},
			run: func(_ context.Context, _ string, arguments ...string) error {
				started <- arguments[3]
				<-release
				return nil
			},
		}
		done <- harness.startTunnels(t.Context())
	}()

	ports := []string{<-started, <-started}
	assert.ElementsMatch(t, []string{"80", "2489"}, ports)
	close(release)
	require.NoError(t, <-done)
}

func TestStartLemonadeServerRunsAllowLocalhost(t *testing.T) {
	var commandName string
	var commandArguments []string
	harness := commandHarness{
		run: func(_ context.Context, name string, arguments ...string) error {
			commandName = name
			commandArguments = arguments
			return nil
		},
	}

	require.NoError(t, harness.startLemonadeServer(t.Context()))
	assert.Equal(t, "lemonade", commandName)
	assert.Equal(t, []string{"server", "-allow", "127.0.0.1"}, commandArguments)
}

func TestValidatePortMappingsIncludesImplicitLemonade(t *testing.T) {
	harness := commandHarness{
		mappings: portMappings{
			{localPort: lemonadePort, workstationPort: 80},
		},
	}

	err := harness.validatePortMappings()
	require.Error(t, err)
	assert.ErrorContains(t, err, "local port 2489 is published more than once")
}
