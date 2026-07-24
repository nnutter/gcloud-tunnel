package main

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"charm.land/fang/v2"
)

const maximumErrorOutput = 8 * 1024

type dashboardError struct {
	err error
}

func (dashboardError dashboardError) Error() string {
	return dashboardError.err.Error()
}

func (dashboardError dashboardError) Unwrap() error {
	return dashboardError.err
}

type tailBuffer struct {
	data []byte
}

func (buffer *tailBuffer) Write(data []byte) (int, error) {
	written := len(data)
	if len(data) >= maximumErrorOutput {
		buffer.data = append(buffer.data[:0], data[len(data)-maximumErrorOutput:]...)
		return written, nil
	}

	overflow := len(buffer.data) + len(data) - maximumErrorOutput
	if overflow > 0 {
		buffer.data = append(buffer.data[:0], buffer.data[overflow:]...)
	}
	buffer.data = append(buffer.data, data...)
	return written, nil
}

func (buffer *tailBuffer) String() string {
	return string(buffer.data)
}

func gcloudError(err error, output string) error {
	message := strings.TrimSpace(output)
	if message == "" {
		return err
	}
	return fmt.Errorf("%w\n%s", err, message)
}

func renderError(w io.Writer, styles fang.Styles, err error) {
	if _, ok := errors.AsType[dashboardError](err); ok {
		return
	}
	fang.DefaultErrorHandler(w, styles, err)
}
