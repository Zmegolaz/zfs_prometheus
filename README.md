# zfs_prometheus
A Prometheus exporter for ZFS on Linux, written in Python using only the standard library. Requires OpenZFS 2.3 or later.

## Usage
```
./metrics.py [-b BIND] [-p PORT] [-i SCRAPE_INTERVAL]
```
Metrics are served on `http://<host>:9901/metrics`. Set `-i` to your Prometheus scrape interval, it's used for command timeouts. See `--help` for all options.

## Metrics
| Group | Prefix | Source |
|---|---|---|
| Dataset properties (used, available, compression ratio, quotas, ...) | `zfs_` | `zfs get` |
| Pool and vdev health, error counters, slow I/Os | `zfs_pool_state`, `zfs_vdev_` | `zpool status` |
| ARC and L2ARC stats | `zfs_arc_` | `/proc/spl/kstat/zfs/arcstats` |
| Physical I/O per vdev | `zfs_vdev_*_ops_total`, `zfs_vdev_*_bytes_total` | `zpool get ... all-vdevs` |
| User I/O per dataset | `zfs_dataset_` | `/proc/spl/kstat/zfs/<pool>/objset-*` |
| ARC-level I/O per pool | `zfs_pool_arc_`, `zfs_pool_direct_` | `/proc/spl/kstat/zfs/<pool>/iostats` |

### Which I/O metric to use
- **Physical I/O** (`zfs_vdev_*_ops_total`): what the disks actually do, including internal work like snapshot destroy, scrub and resilver. Matches `zpool iostat`. Sum either top-level vdevs or leaf disks (`vdev_type=~"disk|file"`), never both, and use the `class` label to separate for example special vdevs.
- **User I/O** (`zfs_dataset_*`): only what users request, before compression, parity and metadata. Writing 1 GB in 10 seconds shows 100 MB/s. Includes reads served from ARC. Only mounted filesystems and active zvols are counted.
- **ARC-level I/O** (`zfs_pool_arc_*`): reads count ARC misses from any source, writes count only user writes. Mostly useful for ARC analysis.

Aggregate in PromQL, for example:
```promql
sum by (pool) (rate(zfs_dataset_write_bytes_total[$__rate_interval]))
```

## Grafana
`grafana.json` is a dashboard that can be imported directly in Grafana.
