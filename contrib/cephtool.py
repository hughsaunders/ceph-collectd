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

def cephtool_read(data=None):
    info = cephtool_subprocess(["osd", "dump"])
    lines = info.splitlines()
    first_json_line = -1
    line_idx = 0
    for line in lines:
        if ((len(line) > 0) and (line[0] == '{')):
            first_json_line = line_idx
            break
        line_idx = line_idx + 1
    if (first_json_line == -1):
        raise Exception("failed to find the first JSON line in the output!")
    jsonstr = "".join(lines[first_json_line:])
    j = json.loads(jsonstr)

    collectd.Values(plugin="cephtool",\
        type='num_osds',\
        values=[len(j["osds"])]\
    ).dispatch()

collectd.register_config(cephtool_config)
collectd.register_read(cephtool_read)
