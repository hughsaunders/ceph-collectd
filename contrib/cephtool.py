import collectd
import json
import os
import random
import subprocess
import sys
import time

g_cephtool_path = ""
g_ceph_config = ""

def cephtool_config(config):
    global g_cephtool_path, g_ceph_config
    for child in config.children:
        if child.key == "cephtool":
            g_cephtool_path = child.values[0]
        elif child.key == "config":
            g_ceph_config = child.values[0]
    collectd.info("cephtool_config: g_cephtool_path='%s', g_ceph_config='%s'" % \
            (g_cephtool_path, g_ceph_config))
    if g_cephtool_path == "": 
        raise Exception("You must configure the path to cephtool.")
    if not os.path.exists(g_cephtool_path):
        raise Exception("Cannot locate cephtool. cephtool is configured as \
'%s', but that does not exist." % g_cephtool_path)

def cephtool_subprocess(more_args):
    args = [g_cephtool_path]
    if (g_ceph_config != ""):
        args.extend(["-c", g_ceph_config])
    args.extend(more_args)
    args.extend(["--format=json", "-o", "-"])
    proc = subprocess.Popen(args, shell=False, stdout=subprocess.PIPE)
    return proc.communicate()[0]

def cephtool_get_json(more_args):
    info = cephtool_subprocess(more_args)
    lines = info.splitlines()
    first_json_line = -1
    line_idx = 0
    for line in lines:
        if ((len(line) > 0) and ((line[0] == '{') or (line[0] == '['))):
            first_json_line = line_idx
            break
        line_idx = line_idx + 1
    if (first_json_line == -1):
        raise Exception("failed to find the first JSON line in the output!")
    jsonstr = "".join(lines[first_json_line:])
    return json.loads(jsonstr)

def cephtool_read(data=None):
    osd_json = cephtool_get_json(["osd", "dump"])
    pg_json = cephtool_get_json(["pg", "dump"])

    collectd.Values(plugin="cephtool",\
        type='num_osds',\
        values=[len(osd_json["osds"])]\
    ).dispatch()
    # number of osds up
    # number of osds down
    # number of osds in
    # number of osds out
    # df info: total disk available
    # df info: total disk used
    # df info: total disk free
    collectd.Values(plugin="cephtool",\
        type='num_pools',\
        values=[len(pg_json["pool_stats"])]\
    ).dispatch()
    collectd.Values(plugin="cephtool",\
        type='num_objects',\
        values=[pg_json["pg_stats_sum"]["num_objects"]]\
    ).dispatch()
    collectd.Values(plugin="cephtool",\
        type='num_bytes',\
        values=[pg_json["pg_stats_sum"]["num_bytes"]]\
    ).dispatch()
    collectd.Values(plugin="cephtool",\
        type='num_pgs',\
        values=[len(pg_json["pg_stats"])]\
    ).dispatch()
    # number of PGs in each state
    # number of monitors
    # number of monitors in quorum
    collectd.Values(plugin="cephtool",\
        type='num_objects_missing_on_primary',\
        values=[pg_json["pg_stats_sum"]["num_objects_missing_on_primary"]]\
    ).dispatch()
    collectd.Values(plugin="cephtool",\
        type='num_objects_degraded',\
        values=[pg_json["pg_stats_sum"]["num_objects_degraded"]]\
    ).dispatch()
    collectd.Values(plugin="cephtool",\
        type='num_objects_unfound',\
        values=[pg_json["pg_stats_sum"]["num_objects_unfound"]]\
    ).dispatch()

collectd.register_config(cephtool_config)
collectd.register_read(cephtool_read)
