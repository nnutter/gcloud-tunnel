# gcloud-tunnel

`gcloud-tunnel` publishes local TCP ports to a Google Cloud Workstation.
It starts one `gcloud workstations start-tcp-tunnel` process for each published port.
An inline dashboard displays every mapping instead of `gcloud` command output.

## Install

```console
go install github.com/nnutter/gcloud-tunnel@latest
```

## Usage

```console
gcloud-tunnel WORKSTATION \
  --cluster CLUSTER \
  --config CONFIG \
  --region REGION \
  --publish LOCAL_PORT:WORKSTATION_PORT
```

`-p` is a shorthand for `--publish` and may be specified multiple times.
The local side is always bound to `localhost`.

```console
gcloud-tunnel my-workstation \
  --project my-project \
  --account developer@example.com \
  --cluster my-cluster \
  --config my-config \
  --region us-central1 \
  --start-workstation \
  -p 8080:80 \
  -p 5432:5432
```

The example forwards `localhost:8080` to workstation port `80` and `localhost:5432` to workstation port `5432`.

`--cluster`, `--config`, `--region`, and at least one `--publish` mapping are required.
Local ports and workstation ports must each be unique.

## Status

The dashboard checks every published mapping every two seconds through a separate hidden tunnel.
The hidden tunnel maps an ephemeral local port to the same workstation port and is never used for your traffic.

- `STARTING`: The tunnel process has started but the first check has not completed.
- `OPEN`: The hidden probe tunnel returned data or stayed connected for the one-second TCP check.
- `WAITING`: The hidden probe tunnel refused the check or closed it immediately. After three
  consecutive failed checks for a tunnel that was previously open, all tunnels restart
  automatically to recover from stale connections after sleep or a network change.
- `UNKNOWN`: The hidden probe tunnel exited, so the workstation port cannot be checked safely.
- `STOPPED`: The tunnel command exited.

The TCP check exercises the forwarding path and detects immediate upstream connection closure without connecting through your visible local port.
It does not verify application-level health, such as an HTTP response or database protocol handshake.

Press `q` or `Ctrl+C` to stop all tunnels.
