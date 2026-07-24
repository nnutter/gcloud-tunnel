package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"charm.land/fang/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTailBufferRetainsMostRecentErrorOutput(t *testing.T) {
	var buffer tailBuffer
	output := strings.Repeat("a", maximumErrorOutput) + "gcloud diagnostic"

	written, err := buffer.Write([]byte(output))

	require.NoError(t, err)
	assert.Equal(t, len(output), written)
	assert.Len(t, buffer.String(), maximumErrorOutput)
	assert.True(t, strings.HasSuffix(buffer.String(), "gcloud diagnostic"))
}

func TestGcloudErrorIncludesDiagnosticOutput(t *testing.T) {
	err := gcloudError(errors.New("exit status 1"), "ERROR: workstation is unavailable\n")

	assert.Equal(t, "exit status 1\nERROR: workstation is unavailable", err.Error())
}

func TestGcloudErrorKeepsProcessErrorWithoutOutput(t *testing.T) {
	processError := errors.New("exit status 1")

	err := gcloudError(processError, "\n\t")

	assert.ErrorIs(t, err, processError)
}

func TestRenderErrorSuppressesDashboardError(t *testing.T) {
	var output bytes.Buffer

	renderError(&output, fang.Styles{}, dashboardError{err: errors.New("already rendered")})

	assert.Empty(t, output.String())
}
