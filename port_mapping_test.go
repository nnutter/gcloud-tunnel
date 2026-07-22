package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePortMapping(t *testing.T) {
	testCases := []struct {
		name    string
		input   string
		mapping portMapping
		message string
	}{
		{
			name:    "valid mapping",
			input:   "8080:80",
			mapping: portMapping{localPort: 8080, workstationPort: 80},
		},
		{
			name:    "missing separator",
			input:   "8080",
			message: "must use LOCAL_PORT:WORKSTATION_PORT",
		},
		{
			name:    "extra separator",
			input:   "8080:80:443",
			message: "must use LOCAL_PORT:WORKSTATION_PORT",
		},
		{
			name:    "zero local port",
			input:   "0:80",
			message: "local port \"0\" must be between 1 and 65535",
		},
		{
			name:    "zero workstation port",
			input:   "8080:0",
			message: "workstation port \"0\" must be between 1 and 65535",
		},
		{
			name:    "out of range port",
			input:   "65536:80",
			message: "local port \"65536\" must be between 1 and 65535",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			mapping, err := parsePortMapping(testCase.input)
			if testCase.message != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, testCase.message)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, testCase.mapping, mapping)
		})
	}
}

func TestPortMappingsSet(t *testing.T) {
	var mappings portMappings

	require.NoError(t, mappings.Set("8080:80"))
	require.NoError(t, mappings.Set("5432:5432"))

	assert.Equal(t, portMappings{
		{localPort: 8080, workstationPort: 80},
		{localPort: 5432, workstationPort: 5432},
	}, mappings)
	assert.Equal(t, "8080:80,5432:5432", mappings.String())
	assert.Equal(t, "port-mapping", mappings.Type())
}
