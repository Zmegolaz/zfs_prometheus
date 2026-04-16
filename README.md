# zfs_prometheus
An exporter that uses python to expose ZFS metrics.

For pool data it uses the `zfs` and `zpool` commands. For ARC stats it reads `/proc/spl/kstat/zfs/arcstats` (configurable).

# Grafana
This repo also includes a Grafana dashboard in JSON format, which can be imported directly in Grafana.
