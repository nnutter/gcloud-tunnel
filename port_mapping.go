package main

import (
	"fmt"
	"strconv"
	"strings"
)

type portMapping struct {
	localPort       uint16
	workstationPort uint16
}

func (mapping portMapping) String() string {
	return fmt.Sprintf("%d:%d", mapping.localPort, mapping.workstationPort)
}

type portMappings []portMapping

func (mappings *portMappings) Set(value string) error {
	mapping, err := parsePortMapping(value)
	if err != nil {
		return err
	}
	*mappings = append(*mappings, mapping)
	return nil
}

func (mappings portMappings) String() string {
	values := make([]string, len(mappings))
	for index, mapping := range mappings {
		values[index] = mapping.String()
	}
	return strings.Join(values, ",")
}

func (portMappings) Type() string {
	return "port-mapping"
}

func parsePortMapping(value string) (portMapping, error) {
	ports := strings.Split(value, ":")
	if len(ports) != 2 {
		return portMapping{}, fmt.Errorf("port mapping %q must use LOCAL_PORT:WORKSTATION_PORT", value)
	}

	localPort, err := parsePort(ports[0], "local")
	if err != nil {
		return portMapping{}, err
	}

	workstationPort, err := parsePort(ports[1], "workstation")
	if err != nil {
		return portMapping{}, err
	}

	return portMapping{localPort: localPort, workstationPort: workstationPort}, nil
}

func parsePort(value, name string) (uint16, error) {
	port, err := strconv.ParseUint(value, 10, 16)
	if err != nil || port == 0 {
		return 0, fmt.Errorf("%s port %q must be between 1 and 65535", name, value)
	}
	return uint16(port), nil
}
