#!/usr/bin/env python3

import json
import logging
import subprocess
from pathlib import Path
from typing import Any
import argparse
import re
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_host(value):
    pat = re.compile(r"^(([0-9a-zA-Z_.-]|[0-9a-fA-F:]{2,39})+)")
    if not pat.match(value):
        raise argparse.ArgumentTypeError("invalid value")
    return value

parser = argparse.ArgumentParser(description='ZFS exporter for Prometheus', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-b', '--bind', type=validate_host, help='Bind to ip/host', default="0.0.0.0")
parser.add_argument('-p', '--port', type=int, help='Listening port', default=9901)
parser.add_argument('-a', '--arcstats', type=str, help='Path for ZFS arcstats', default="/proc/spl/kstat/zfs/arcstats")

args = parser.parse_args()

ARCSTATS_PATH = Path(args.arcstats)

ZFS_DATASET_METRICS: dict[str, tuple[str, str, str]] = {
    "used":                 ("zfs_used_bytes",                   "gauge",   "Bytes used by dataset and all descendants"),
    "available":            ("zfs_available_bytes",              "gauge",   "Bytes available to dataset"),
    "referenced":           ("zfs_referenced_bytes",             "gauge",   "Bytes directly referenced by dataset"),
    "logicalused":          ("zfs_logical_used_bytes",           "gauge",   "Logical bytes used (uncompressed)"),
    "logicalreferenced":    ("zfs_logical_referenced_bytes",     "gauge",   "Logical bytes referenced (uncompressed)"),
    "compressratio":        ("zfs_compress_ratio",               "gauge",   "Compression ratio achieved"),
    "usedbysnapshots":      ("zfs_used_by_snapshots_bytes",      "gauge",   "Bytes used by snapshots"),
    "usedbydataset":        ("zfs_used_by_dataset_bytes",        "gauge",   "Bytes used by the dataset itself"),
    "usedbychildren":       ("zfs_used_by_children_bytes",       "gauge",   "Bytes used by child datasets"),
    "usedbyrefreservation": ("zfs_used_by_refreservation_bytes", "gauge",   "Bytes used by refreservation"),
    "written":              ("zfs_written_bytes",                "gauge",   "Bytes written since last snapshot"),
    "recordsize":           ("zfs_record_size_bytes",            "gauge",   "Configured record size"),
    "quota":                ("zfs_quota_bytes",                  "gauge",   "Dataset quota in bytes (0 = none)"),
    "reservation":          ("zfs_reservation_bytes",            "gauge",   "Dataset reservation in bytes (0 = none)"),
    "refquota":             ("zfs_refquota_bytes",               "gauge",   "Referenced quota in bytes (0 = none)"),
    "refreservation":       ("zfs_refreservation_bytes",         "gauge",   "Referenced reservation in bytes (0 = none)"),
}

ARC_METRICS: dict[str, tuple[str, str, str]] = {
    "hits":                             ("zfs_arc_hits",                             "counter",     "total ARC cache hits"),
    "iohits":                           ("zfs_arc_iohits",                           "counter",     "hits satisfied by in-flight or recently completed I/O"),
    "misses":                           ("zfs_arc_misses",                           "counter",     "total ARC cache misses"),

    "demand_data_hits":                 ("zfs_arc_demand_data_hits",                 "counter",     "demand data hits in ARC"),
    "demand_data_iohits":               ("zfs_arc_demand_data_iohits",               "counter",     "demand data hits satisfied from in-flight I/O"),
    "demand_data_misses":               ("zfs_arc_demand_data_misses",               "counter",     "demand data misses requiring fetch"),

    "demand_metadata_hits":             ("zfs_arc_demand_metadata_hits",             "counter",     "metadata demand hits in ARC"),
    "demand_metadata_iohits":           ("zfs_arc_demand_metadata_iohits",           "counter",     "metadata demand hits from in-flight I/O"),
    "demand_metadata_misses":           ("zfs_arc_demand_metadata_misses",           "counter",     "metadata demand misses requiring fetch"),

    "prefetch_data_hits":               ("zfs_arc_prefetch_data_hits",               "counter",     "prefetch data hits in ARC"),
    "prefetch_data_iohits":             ("zfs_arc_prefetch_data_iohits",             "counter",     "prefetch data hits from in-flight I/O"),
    "prefetch_data_misses":             ("zfs_arc_prefetch_data_misses",             "counter",     "prefetch data misses"),

    "prefetch_metadata_hits":           ("zfs_arc_prefetch_metadata_hits",           "counter",     "prefetch metadata hits in ARC"),
    "prefetch_metadata_iohits":         ("zfs_arc_prefetch_metadata_iohits",         "counter",     "prefetch metadata hits from in-flight I/O"),
    "prefetch_metadata_misses":         ("zfs_arc_prefetch_metadata_misses",         "counter",     "prefetch metadata misses"),

    "mru_hits":                         ("zfs_arc_mru_hits",                         "counter",     "hits in the MRU cache"),
    "mru_ghost_hits":                   ("zfs_arc_mru_ghost_hits",                   "counter",     "hits in MRU ghost list"),

    "mfu_hits":                         ("zfs_arc_mfu_hits",                         "counter",     "hits in the MFU cache"),
    "mfu_ghost_hits":                   ("zfs_arc_mfu_ghost_hits",                   "counter",     "hits in MFU ghost list"),

    "uncached_hits":                    ("zfs_arc_uncached_hits",                    "counter",     "hits for uncached ARC data"),
    "deleted":                          ("zfs_arc_deleted",                          "counter",     "number of ARC buffers deleted"),

    "mutex_miss":                       ("zfs_arc_mutex_miss",                       "counter",     "ARC lock contention misses"),
    "access_skip":                      ("zfs_arc_access_skip",                      "counter",     "ARC access skipped due to constraints"),
    "evict_skip":                       ("zfs_arc_evict_skip",                       "counter",     "evictions skipped due to constraints"),
    "evict_not_enough":                 ("zfs_arc_evict_not_enough",                 "counter",     "insufficient reclaim during eviction"),

    "evict_l2_cached":                  ("zfs_arc_evict_l2_cached",                  "counter",     "bytes already in L2ARC at eviction time"),

    "evict_l2_eligible":                ("zfs_arc_evict_l2_eligible",                "gauge",       "bytes eligible for L2ARC eviction"),
    "evict_l2_eligible_mfu":            ("zfs_arc_evict_l2_eligible_mfu",            "gauge",       "MFU portion eligible for L2ARC"),
    "evict_l2_eligible_mru":            ("zfs_arc_evict_l2_eligible_mru",            "gauge",       "MRU portion eligible for L2ARC"),
    "evict_l2_ineligible":              ("zfs_arc_evict_l2_ineligible",              "gauge",       "bytes not eligible for L2ARC eviction"),

    "evict_l2_skip":                    ("zfs_arc_evict_l2_skip",                    "counter",     "L2ARC eviction attempts skipped"),

    "hash_elements":                    ("zfs_arc_hash_elements",                    "gauge",       "current ARC hash entries"),
    "hash_elements_max":                ("zfs_arc_hash_elements_max",                "counter",     "peak hash entries"),
    "hash_collisions":                  ("zfs_arc_hash_collisions",                  "counter",     "hash collisions encountered"),
    "hash_chains":                      ("zfs_arc_hash_chains",                      "gauge",       "number of hash chains in use"),
    "hash_chain_max":                   ("zfs_arc_hash_chain_max",                   "counter",     "maximum chain length observed"),

    "meta":                             ("zfs_arc_meta",                             "gauge",       "ARC metadata accounting"),
    "pd":                               ("zfs_arc_pd",                               "gauge",       "internal ARC accounting (data)"),
    "pm":                               ("zfs_arc_pm",                               "gauge",       "internal ARC accounting (metadata)"),

    "c":                                ("zfs_arc_c",                                "gauge",       "ARC target size"),
    "c_min":                            ("zfs_arc_c_min",                            "gauge",       "minimum ARC size"),
    "c_max":                            ("zfs_arc_c_max",                            "gauge",       "maximum ARC size"),

    "size":                             ("zfs_arc_size",                             "gauge",       "current ARC size in bytes"),
    "compressed_size":                  ("zfs_arc_compressed_size",                  "gauge",       "compressed data in ARC"),
    "uncompressed_size":                ("zfs_arc_uncompressed_size",                "gauge",       "logical uncompressed ARC data size"),
    "overhead_size":                    ("zfs_arc_overhead_size",                    "gauge",       "ARC overhead bytes"),

    "hdr_size":                         ("zfs_arc_hdr_size",                         "gauge",       "ARC buffer header memory usage"),
    "data_size":                        ("zfs_arc_data_size",                        "gauge",       "ARC cached data size"),
    "metadata_size":                    ("zfs_arc_metadata_size",                    "gauge",       "ARC cached metadata size"),
    "dbuf_size":                        ("zfs_arc_dbuf_size",                        "gauge",       "dmu buffer cache size"),
    "dnode_size":                       ("zfs_arc_dnode_size",                       "gauge",       "dnode cache size"),
    "bonus_size":                       ("zfs_arc_bonus_size",                       "gauge",       "bonus buffer size"),

    "anon_size":                        ("zfs_arc_anon_size",                        "gauge",       "anonymous ARC buffers size"),
    "anon_data":                        ("zfs_arc_anon_data",                        "gauge",       "anonymous data buffers"),
    "anon_metadata":                    ("zfs_arc_anon_metadata",                    "gauge",       "anonymous metadata buffers"),
    "anon_evictable_data":              ("zfs_arc_anon_evictable_data",              "gauge",       "evictable anonymous data"),
    "anon_evictable_metadata":          ("zfs_arc_anon_evictable_metadata",          "gauge",       "evictable anonymous metadata"),

    "mru_size":                         ("zfs_arc_mru_size",                         "gauge",       "MRU state size"),
    "mru_data":                         ("zfs_arc_mru_data",                         "gauge",       "MRU data size"),
    "mru_metadata":                     ("zfs_arc_mru_metadata",                     "gauge",       "MRU metadata size"),
    "mru_evictable_data":               ("zfs_arc_mru_evictable_data",               "gauge",       "MRU evictable data"),
    "mru_evictable_metadata":           ("zfs_arc_mru_evictable_metadata",           "gauge",       "MRU evictable metadata"),

    "mru_ghost_size":                   ("zfs_arc_mru_ghost_size",                   "gauge",       "MRU ghost list size"),
    "mru_ghost_data":                   ("zfs_arc_mru_ghost_data",                   "gauge",       "MRU ghost data tracking"),
    "mru_ghost_metadata":               ("zfs_arc_mru_ghost_metadata",               "gauge",       "MRU ghost metadata tracking"),
    "mru_ghost_evictable_data":         ("zfs_arc_mru_ghost_evictable_data",         "gauge",       "MRU ghost evictable data"),
    "mru_ghost_evictable_metadata":     ("zfs_arc_mru_ghost_evictable_metadata",     "gauge",       "MRU ghost evictable metadata"),

    "mfu_size":                         ("zfs_arc_mfu_size",                         "gauge",       "MFU state size"),
    "mfu_data":                         ("zfs_arc_mfu_data",                         "gauge",       "MFU data size"),
    "mfu_metadata":                     ("zfs_arc_mfu_metadata",                     "gauge",       "MFU metadata size"),
    "mfu_evictable_data":               ("zfs_arc_mfu_evictable_data",               "gauge",       "MFU evictable data"),
    "mfu_evictable_metadata":           ("zfs_arc_mfu_evictable_metadata",           "gauge",       "MFU evictable metadata"),

    "mfu_ghost_size":                   ("zfs_arc_mfu_ghost_size",                   "gauge",       "MFU ghost list size"),
    "mfu_ghost_data":                   ("zfs_arc_mfu_ghost_data",                   "gauge",       "MFU ghost data tracking"),
    "mfu_ghost_metadata":               ("zfs_arc_mfu_ghost_metadata",               "gauge",       "MFU ghost metadata tracking"),
    "mfu_ghost_evictable_data":         ("zfs_arc_mfu_ghost_evictable_data",         "gauge",       "MFU ghost evictable data"),
    "mfu_ghost_evictable_metadata":     ("zfs_arc_mfu_ghost_evictable_metadata",     "gauge",       "MFU ghost evictable metadata"),

    "uncached_size":                    ("zfs_arc_uncached_size",                    "gauge",       "uncached ARC size"),
    "uncached_data":                    ("zfs_arc_uncached_data",                    "gauge",       "uncached data"),
    "uncached_metadata":                ("zfs_arc_uncached_metadata",                "gauge",       "uncached metadata"),
    "uncached_evictable_data":          ("zfs_arc_uncached_evictable_data",          "gauge",       "uncached evictable data"),
    "uncached_evictable_metadata":      ("zfs_arc_uncached_evictable_metadata",      "gauge",       "uncached evictable metadata"),

    "l2_hits":                          ("zfs_arc_l2_hits",                          "counter",     "L2ARC hits"),
    "l2_misses":                        ("zfs_arc_l2_misses",                        "counter",     "L2ARC misses"),

    "l2_prefetch_asize":                ("zfs_arc_l2_prefetch_asize",                "gauge",       "L2ARC prefetch allocated size"),
    "l2_mru_asize":                     ("zfs_arc_l2_mru_asize",                     "gauge",       "L2ARC MRU evicted size"),
    "l2_mfu_asize":                     ("zfs_arc_l2_mfu_asize",                     "gauge",       "L2ARC MFU evicted size"),
    "l2_bufc_data_asize":               ("zfs_arc_l2_bufc_data_asize",               "gauge",       "L2ARC buffered data size"),
    "l2_bufc_metadata_asize":           ("zfs_arc_l2_bufc_metadata_asize",           "gauge",       "L2ARC buffered metadata size"),

    "l2_feeds":                         ("zfs_arc_l2_feeds",                         "counter",     "L2ARC feeds"),
    "l2_rw_clash":                      ("zfs_arc_l2_rw_clash",                      "counter",     "L2ARC read/write contention"),

    "l2_read_bytes":                    ("zfs_arc_l2_read_bytes",                    "counter",     "L2ARC bytes read"),
    "l2_write_bytes":                   ("zfs_arc_l2_write_bytes",                   "counter",     "L2ARC bytes written"),

    "l2_writes_sent":                   ("zfs_arc_l2_writes_sent",                   "counter",     "L2ARC writes sent"),
    "l2_writes_done":                   ("zfs_arc_l2_writes_done",                   "counter",     "L2ARC writes completed"),
    "l2_writes_error":                  ("zfs_arc_l2_writes_error",                  "counter",     "L2ARC write errors"),

    "l2_writes_lock_retry":             ("zfs_arc_l2_writes_lock_retry",             "counter",     "L2ARC write lock retries"),
    "l2_evict_lock_retry":              ("zfs_arc_l2_evict_lock_retry",              "counter",     "L2ARC eviction lock retries"),
    "l2_evict_reading":                 ("zfs_arc_l2_evict_reading",                 "counter",     "L2ARC eviction delayed by reads"),
    "l2_evict_l1cached":                ("zfs_arc_l2_evict_l1cached",                "counter",     "L2ARC evicted due to L1 residency"),
    "l2_free_on_write":                 ("zfs_arc_l2_free_on_write",                 "counter",     "blocks freed during L2ARC write"),

    "l2_abort_lowmem":                  ("zfs_arc_l2_abort_lowmem",                  "counter",     "L2ARC aborted due to low memory"),
    "l2_cksum_bad":                     ("zfs_arc_l2_cksum_bad",                     "counter",     "L2ARC checksum errors"),
    "l2_io_error":                      ("zfs_arc_l2_io_error",                      "counter",     "L2ARC I/O errors"),

    "l2_size":                          ("zfs_arc_l2_size",                          "gauge",       "usable L2ARC size"),
    "l2_asize":                         ("zfs_arc_l2_asize",                         "gauge",       "allocated L2ARC size"),
    "l2_hdr_size":                      ("zfs_arc_l2_hdr_size",                      "gauge",       "L2ARC header overhead"),

    "l2_log_blk_writes":                ("zfs_arc_l2_log_blk_writes",                "counter",     "L2ARC log block writes"),
    "l2_log_blk_avg_asize":             ("zfs_arc_l2_log_blk_avg_asize",             "gauge",       "average L2ARC log block size"),
    "l2_log_blk_asize":                 ("zfs_arc_l2_log_blk_asize",                 "gauge",       "L2ARC log block size"),
    "l2_log_blk_count":                 ("zfs_arc_l2_log_blk_count",                 "gauge",       "L2ARC log block count"),

    "l2_data_to_meta_ratio":            ("zfs_arc_l2_data_to_meta_ratio",            "gauge",       "L2ARC data/metadata ratio"),

    "l2_rebuild_success":               ("zfs_arc_l2_rebuild_success",               "counter",     "L2ARC rebuild success count"),
    "l2_rebuild_unsupported":           ("zfs_arc_l2_rebuild_unsupported",           "counter",     "L2ARC rebuild unsupported"),
    "l2_rebuild_io_errors":             ("zfs_arc_l2_rebuild_io_errors",             "counter",     "L2ARC rebuild I/O errors"),
    "l2_rebuild_dh_errors":             ("zfs_arc_l2_rebuild_dh_errors",             "counter",     "L2ARC rebuild data/header errors"),
    "l2_rebuild_cksum_lb_errors":       ("zfs_arc_l2_rebuild_cksum_lb_errors",       "counter",     "L2ARC rebuild checksum errors"),
    "l2_rebuild_lowmem":                ("zfs_arc_l2_rebuild_lowmem",                "counter",     "L2ARC rebuild low memory aborts"),

    "l2_rebuild_size":                  ("zfs_arc_l2_rebuild_size",                  "gauge",       "L2ARC rebuild size"),
    "l2_rebuild_asize":                 ("zfs_arc_l2_rebuild_asize",                 "gauge",       "L2ARC rebuild allocated size"),
    "l2_rebuild_bufs":                  ("zfs_arc_l2_rebuild_bufs",                  "counter",     "L2ARC rebuild buffers processed"),
    "l2_rebuild_bufs_precached":        ("zfs_arc_l2_rebuild_bufs_precached",        "counter",     "L2ARC rebuild precached buffers"),
    "l2_rebuild_log_blks":              ("zfs_arc_l2_rebuild_log_blks",              "counter",     "L2ARC rebuild log blocks"),

    "memory_throttle_count":            ("zfs_arc_memory_throttle_count",            "counter",     "ARC memory throttle events"),
    "memory_direct_count":              ("zfs_arc_memory_direct_count",              "counter",     "direct memory reclaim events"),
    "memory_indirect_count":            ("zfs_arc_memory_indirect_count",            "counter",     "indirect memory reclaim events"),

    "memory_all_bytes":                 ("zfs_arc_memory_all_bytes",                 "gauge",       "total system memory"),
    "memory_free_bytes":                ("zfs_arc_memory_free_bytes",                "gauge",       "free system memory"),
    "memory_available_bytes":           ("zfs_arc_memory_available_bytes",           "gauge",       "available memory for ARC"),

    "arc_no_grow":                      ("zfs_arc_arc_no_grow",                      "counter",     "ARC growth blocked events"),
    "arc_tempreserve":                  ("zfs_arc_arc_tempreserve",                  "counter",     "ARC temporary reservations"),
    "arc_loaned_bytes":                 ("zfs_arc_arc_loaned_bytes",                 "gauge",       "ARC loaned bytes"),

    "arc_prune":                        ("zfs_arc_arc_prune",                        "counter",     "ARC prune events"),

    "arc_meta_used":                    ("zfs_arc_arc_meta_used",                    "gauge",       "ARC metadata used"),
    "arc_dnode_limit":                  ("zfs_arc_arc_dnode_limit",                  "gauge",       "ARC dnode limit"),

    "async_upgrade_sync":               ("zfs_arc_async_upgrade_sync",               "counter",     "async upgrade sync events"),

    "predictive_prefetch":              ("zfs_arc_predictive_prefetch",              "counter",     "predictive prefetch attempts"),
    "demand_hit_predictive_prefetch":   ("zfs_arc_demand_hit_predictive_prefetch",   "counter",     "predictive prefetch demand hits"),
    "demand_iohit_predictive_prefetch": ("zfs_arc_demand_iohit_predictive_prefetch", "counter",     "predictive prefetch IO hits"),

    "prescient_prefetch":               ("zfs_arc_prescient_prefetch",               "counter",     "prescient prefetch attempts"),
    "demand_hit_prescient_prefetch":    ("zfs_arc_demand_hit_prescient_prefetch",    "counter",     "prescient prefetch demand hits"),
    "demand_iohit_prescient_prefetch":  ("zfs_arc_demand_iohit_prescient_prefetch",  "counter",     "prescient prefetch IO hits"),

    "arc_need_free":                    ("zfs_arc_arc_need_free",                    "counter",     "ARC forced free events"),
    "arc_sys_free":                     ("zfs_arc_arc_sys_free",                     "gauge",       "system free memory influence metric"),
    "arc_raw_size":                     ("zfs_arc_arc_raw_size",                     "gauge",       "raw ARC allocation size"),

    "cached_only_in_progress":          ("zfs_arc_cached_only_in_progress",          "counter",     "cached-only operations in progress"),
    "abd_chunk_waste_size":             ("zfs_arc_abd_chunk_waste_size",             "gauge",       "ABD chunk wasted space"),
}

# vdev_type values that represent physical leaf devices we want to track.
# "disk" and "file" are leaves; "mirror", "raidz", "draid", "spare", "l2cache",
# "log" etc. are interior/virtual.
LEAF_VDEV_TYPES = {"disk", "file"}

VDEV_STATE_MAP: dict[str, int] = {
    "online":       0,
    "avail":        0,  # Spares are avail, not online
    "degraded":     1,
    "faulted":      2,
    "offline":      3,
    "removed":      4,
    "unavail":      5,
    "unknown":      6,
}


def read_arcstats() -> dict[str, int]:
    stats: dict[str, int] = {}
    for line in ARCSTATS_PATH.read_text().splitlines():
        parts = line.split()
        if len(parts) == 3 and parts[1].isdigit():
            try:
                stats[parts[0]] = int(parts[2])
            except ValueError:
                pass
    return stats


def run_zfs_get_all() -> dict[str, Any]:
    result = subprocess.run(
        ["zfs", "get", "-pj", "all"],
        capture_output=True, text=True, check=True,
    )
    return json.loads(result.stdout)


def run_zpool_status() -> dict[str, Any]:
    # --json-int        -> numbers as integers (read_errors etc.)
    # --json-flat-vdevs -> flat dict keyed by guid, each entry has a "vdevs"
    #                      child list so we still know the tree; but actually
    #                      flat mode loses hierarchy context we need for the
    #                      "vdev_type" of parents.  Use nested (default) instead
    #                      and recurse ourselves.
    result = subprocess.run(
        ["zpool", "status", "-j", "--json-int"],
        capture_output=True, text=True, check=True,
    )
    return json.loads(result.stdout)


def is_snapshot(name: str) -> bool:
    return "@" in name


def _iter_vdevs(vdev: dict[str, Any], pool: str) -> list[dict[str, Any]]:
    """
    Recursively walk the nested vdev tree and yield a flat list of dicts
    with the fields we care about.

    The JSON tree looks like:
        pool-root (no vdev_type)
          |- mirror-0 / raidz1-0 / ... (vdev_type: mirror/raidz/...)
               |- <guid> (vdev_type: disk/file)
    """
    results = []
    name = vdev.get("name", "")
    path = vdev.get("path", name)
    vdev_type = vdev.get("vdev_type", "")
    # draid don't have any vdev_type for some reason.
    if not vdev_type:
        if vdev.get("name", "").startswith("draid"):
            vdev_type = "draid"
    # Again, draid is missing this in the JSON data.
    state_raw = vdev.get("state", "unknown")

    results.append({
        "pool":       pool,
        "vdev":       path,
        "vdev_type":  vdev_type,
        "state_raw":  state_raw.lower(),
        "state_int":  VDEV_STATE_MAP.get(state_raw.lower(), 6),
        "read_errors":     int(vdev.get("read_errors", 0)),
        "write_errors":    int(vdev.get("write_errors", 0)),
        "checksum_errors": int(vdev.get("checksum_errors", 0)),
        "slow_ios":   int(vdev.get("slow_ios", 0)),
    })

    for child in vdev.get("vdevs", {}).values():
        results.extend(_iter_vdevs(child, pool))

    return results


def collect_vdev_metrics(lines: list[str]) -> None:
    try:
        data = run_zpool_status()
    except (subprocess.CalledProcessError, KeyError, json.JSONDecodeError) as e:
        logger.warning("zpool status failed: %s", e)
        return

    all_vdevs: list[dict[str, Any]] = []
    for pool_name, pool_info in data.get("pools", {}).items():
        pool_state = pool_info.get("state", "unknown").lower()
        # Pool-level health metric (0 = online)
        pool_state_int = VDEV_STATE_MAP.get(pool_state, 6)
        lines.append(
            f'zfs_pool_state{{pool="{pool_name}"}} {pool_state_int}'
        )

        # Walk trees (the top-level "vdevs" dict contains the root vdev)
        for root_vdev in pool_info.get("vdevs", {}).values():
            all_vdevs.extend(_iter_vdevs(root_vdev, pool_name))
        for root_vdev in pool_info.get("l2cache", {}).values():
            all_vdevs.extend(_iter_vdevs(root_vdev, pool_name))
        for root_vdev in pool_info.get("special", {}).values():
            all_vdevs.extend(_iter_vdevs(root_vdev, pool_name))
        for root_vdev in pool_info.get("spares", {}).values():
            all_vdevs.extend(_iter_vdevs(root_vdev, pool_name))


    if not all_vdevs:
        return

    # zfs_vdev_state: numeric state per vdev
    lines.append("# HELP zfs_vdev_state Vdev health state "
                 "(0=online/avail 1=degraded 2=faulted 3=offline 4=removed 5=unavail 6=unknown)")
    lines.append("# TYPE zfs_vdev_state gauge")
    for v in all_vdevs:
        lines.append(
            f'zfs_vdev_state{{pool="{v["pool"]}",vdev="{v["vdev"]}",'
            f'vdev_type="{v["vdev_type"]}"}} {v["state_int"]}'
        )

    # per-error counters (leaf vdevs only — virtual vdevs aggregate)
    lines.append("# HELP zfs_vdev_read_errors_total Read errors on vdev")
    lines.append("# TYPE zfs_vdev_read_errors_total counter")
    for v in all_vdevs:
        if v["vdev_type"] in LEAF_VDEV_TYPES:
            lines.append(
                f'zfs_vdev_read_errors_total{{pool="{v["pool"]}",vdev="{v["vdev"]}",'
                f'vdev_type="{v["vdev_type"]}"}} {v["read_errors"]}'
            )

    lines.append("# HELP zfs_vdev_write_errors_total Write errors on vdev")
    lines.append("# TYPE zfs_vdev_write_errors_total counter")
    for v in all_vdevs:
        if v["vdev_type"] in LEAF_VDEV_TYPES:
            lines.append(
                f'zfs_vdev_write_errors_total{{pool="{v["pool"]}",vdev="{v["vdev"]}",'
                f'vdev_type="{v["vdev_type"]}"}} {v["write_errors"]}'
            )

    lines.append("# HELP zfs_vdev_checksum_errors_total Checksum errors on vdev")
    lines.append("# TYPE zfs_vdev_checksum_errors_total counter")
    for v in all_vdevs:
        if v["vdev_type"] in LEAF_VDEV_TYPES:
            lines.append(
                f'zfs_vdev_checksum_errors_total{{pool="{v["pool"]}",vdev="{v["vdev"]}",'
                f'vdev_type="{v["vdev_type"]}"}} {v["checksum_errors"]}'
            )

    lines.append("# HELP zfs_vdev_slow_ios_total Slow IOs on vdev")
    lines.append("# TYPE zfs_vdev_slow_ios_total counter")
    for v in all_vdevs:
        if v["vdev_type"] in LEAF_VDEV_TYPES:
            lines.append(
                f'zfs_vdev_slow_ios_total{{pool="{v["pool"]}",vdev="{v["vdev"]}",'
                f'vdev_type="{v["vdev_type"]}"}} {v["slow_ios"]}'
            )

    # summary counts by state (useful for alerting: any non-zero degraded?)
    lines.append("# HELP zfs_pool_vdevs_by_state Number of vdevs in each state per pool")
    lines.append("# TYPE zfs_pool_vdevs_by_state gauge")
    # Count leaf vdevs only for the summary
    from collections import Counter
    for pool_name in {v["pool"] for v in all_vdevs}:
        counts: Counter = Counter()
        for v in all_vdevs:
            if v["pool"] == pool_name and v["vdev_type"] in LEAF_VDEV_TYPES:
                counts[v["state_raw"]] += 1
        for state, count in counts.items():
            lines.append(
                f'zfs_pool_vdevs_by_state{{pool="{pool_name}",state="{state}"}} {count}'
            )


def collect_metrics() -> str:
    lines: list[str] = []

    # Dataset metrics: one subprocess call
    raw = run_zfs_get_all()
    datasets = {
        name: info
        for name, info in raw["datasets"].items()
        if not is_snapshot(name)
    }

    headers_written: set[str] = set()
    for name, info in datasets.items():
        pool = info.get("pool", "unknown")
        props = info.get("properties", {})

        for zfs_prop, (metric_name, metric_type, description) in ZFS_DATASET_METRICS.items():
            if zfs_prop not in props:
                continue
            raw_value = props[zfs_prop]["value"]
            try:
                value = float(raw_value)
            except ValueError:
                continue

            if metric_name not in headers_written:
                lines.append(f"# HELP {metric_name} {description}")
                lines.append(f"# TYPE {metric_name} {metric_type}")
                headers_written.add(metric_name)

            lines.append(f'{metric_name}{{dataset="{name}",pool="{pool}"}} {value}')

    # Pool / vdev health
    lines.append("# HELP zfs_pool_state Pool health state "
                 "(0=online/avail 1=degraded 2=faulted 3=offline 4=removed 5=unavail 6=unknown)")
    lines.append("# TYPE zfs_pool_state gauge")
    collect_vdev_metrics(lines)

    # L2ARC metrics
    try:
        arcstats = read_arcstats()
        for stat_key, (metric_name, metric_type, description) in ARC_METRICS.items():
            if stat_key not in arcstats:
                continue
            lines.append(f"# HELP {metric_name} {description}")
            lines.append(f"# TYPE {metric_name} {metric_type}")
            lines.append(f"{metric_name} {arcstats[stat_key]}")
    except OSError as e:
        logger.warning("Could not read arcstats: %s", e)

    return "\n".join(lines) + "\n"


class RequestHandler(BaseHTTPRequestHandler):
    def accept_request(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()

    def reject_request(self) -> None:
        self.send_response(404)
        self.end_headers()

    def do_GET(self) -> None:
        if self.path == "/metrics":
            self.accept_request()
            self.wfile.write(collect_metrics().encode())
            return
        if self.path == "/healthz":
            self.accept_request()
            self.wfile.write("ok")
            return

        self.reject_request()


if __name__ == "__main__":
    try:
        logger.info("Starting ZFS exporter on %s:%d", args.bind, args.port)
        http_server = ThreadingHTTPServer((args.bind, args.port), RequestHandler)
        http_server.serve_forever()
    except KeyboardInterrupt:
        pass

