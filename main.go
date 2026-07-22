package main

import (
	"context"
	"os"
	"os/exec"
	"syscall"

	"charm.land/fang/v2"
	"github.com/spf13/cobra"
)

func main() {
	command := newCommand()
	err := fang.Execute(
		context.Background(),
		command,
		fang.WithColorSchemeFunc(tunnelColorScheme),
		fang.WithNotifySignal(os.Interrupt, syscall.SIGTERM),
	)
	if err != nil {
		os.Exit(1)
	}
}

func newCommand() *cobra.Command {
	harness := new(commandHarness)
	command := &cobra.Command{
		Use:     "gcloud-tunnel WORKSTATION",
		Short:   "Publish local ports to a Cloud Workstation",
		Example: "gcloud-tunnel workstation --cluster cluster --config config --region us-central1 -p 8080:80",
		Args:    cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, arguments []string) error {
			harness.workstation = arguments[0]

			harness.run = func(ctx context.Context, name string, arg ...string) error {
				cmd := exec.CommandContext(ctx, name, arg...)
				cmd.Stdin = command.InOrStdin()
				cmd.Stdout = command.OutOrStdout()
				cmd.Stderr = command.ErrOrStderr()
				return cmd.Run()
			}

			return harness.startTunnels(command.Context())
		},
	}

	flags := command.Flags()
	flags.StringVar(&harness.account, "account", "", "Google account to use for authentication")
	flags.StringVar(&harness.project, "project", "", "Google Cloud project to use")
	flags.StringVar(&harness.cluster, "cluster", "", "Cluster for the workstation")
	flags.StringVar(&harness.config, "config", "", "Config for the workstation")
	flags.StringVar(&harness.region, "region", "", "Region for the workstation")
	flags.BoolVar(&harness.startWorkstation, "start-workstation", false, "Start the workstation if stopped")

	flags.VarP(&harness.mappings, "publish", "p", "Publish LOCAL_PORT:WORKSTATION_PORT")

	for _, name := range []string{"cluster", "config", "publish", "region"} {
		if err := command.MarkFlagRequired(name); err != nil {
			panic(err)
		}
	}
	return command
}
