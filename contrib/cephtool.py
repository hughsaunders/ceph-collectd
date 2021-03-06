import collectd
import json
import os
import random
import string
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
    collectd.warning("cephtool_config: g_cephtool_path='%s', g_ceph_config='%s'" % \
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
    args.extend(["--format=json"])
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

def cephtool_read_pg_states(pg_json):
    stateinfo = {
        "active" : 0,
        "clean" : 0,
        "crashed" : 0,
        "creating" : 0,
        "degraded" : 0,
        "down" : 0,
        "inconsistent" : 0,
        "peering" : 0,
        "repair" : 0,
        "replay" : 0,
        "scanning" : 0,
        "scrubbing" : 0,
        "scrubq" : 0,
        "splitting" : 0,
        "stray" : 0,
    }
    for pg in pg_json:
        state = pg["state"]
        slist = string.split(state, "+")
        for s in slist:
            if not s in stateinfo:
                collectd.error("PG %s has unknown state %s" % \
                    (pg_json["pgid"], s))
            else:
                stateinfo[s] = stateinfo[s] + 1
    for k,v in stateinfo.items():
        collectd.Values(plugin="ceph.pg",\
            type="gauge",\
            type_instance=('num_pgs_' + k),\
            values=[v]\
        ).dispatch()

def cephtool_read_osd(osd_json):
    num_in = 0
    num_up = 0
    total = 0
    for osd in osd_json:
        total = total + 1
        if osd["in"] == 1:
            num_in = num_in + 1
        if osd["up"] == 1:
            num_up = num_up + 1
    collectd.Values(plugin="ceph.osd",\
        type="gauge",\
        type_instance="num_in",\
        values=[num_in],\
    ).dispatch()
    collectd.Values(plugin="ceph.osd",\
        type="gauge",\
        type_instance="num_out",\
        values=[total - num_in],\
    ).dispatch()
    collectd.Values(plugin="ceph.osd",\
        type="gauge",\
        type_instance="num_up",\
        values=[num_up],\
    ).dispatch()
    collectd.Values(plugin="ceph.osd",\
        type="gauge",\
        type_instance="num_down",\
        values=[total - num_up],\
    ).dispatch()


def cephtool_read_pools(osd_json, pg_json, df_json):
    """Collect per pool metrics"""
    pools = osd_json['pools']
    dfs = df_json
    pgs = pg_json['pg_stats']
    osds = pg_json['osd_stats']

    # Get pool disk usage information from ceph df
    for df in dfs['pools']:
        pool = filter(lambda x: x['pool'] == df['id'], pools)[0]
        pool['pool_stored'] = df['stats']['kb_used']
        pool['pool_used'] = pool['pool_stored'] * pool['size']

        # Clear stored minumums so that increases in OSD space are recognised
        if 'osd_available_min' in pool:
            del pool['osd_available_min']

    # Add OSD space free/available information to each pool
    for pg in pgs:
        pgid = pg['pgid']
        acting = pg['acting']
        pool_num, pg = pgid.split('.')
        pool_num = int(pool_num)
        pool = filter(lambda x: x['pool'] == pool_num, pools)[0]

        for osd_id in acting:
            osd = filter(lambda x: x['osd'] == osd_id, osds)[0]
            if 'OSDs' not in pool:
                pool['OSDs'] = set()
            if 'osd_total' not in pool:
                pool['osd_total'] = 0
            if 'osd_available' not in pool:
                pool['osd_available'] = 0

            if osd_id not in pool['OSDs']:
                pool['OSDs'].add(osd_id)
                pool['osd_total'] += (osd['kb_used'] + osd['kb_avail'])
                pool['osd_available'] += osd['kb_avail']

            if ('osd_available_min' not in pool
                    or osd['kb_avail'] < pool['osd_available_min']):
                pool['osd_available_min'] = osd['kb_avail']

    # Add calculated values
    for pool in pools:
        pool['pool_max'] = pool['osd_total'] / pool['size']
        pool['pool_available'] = pool['osd_available']/pool['size']
        pool['pool_percent'] = (float(pool['pool_used']) /
                                (pool['pool_used'] +
                                 pool['osd_available'])) * 100
        pool['pool_name'] = pool['pool_name'].replace(".","_")
        pool['recommended_pg_num'] = (len(osd_json["osds"]) * 100) / pool['size']

        # Report all the pool's numeric properties as collectd metrics.
        for key, value in pool.iteritems():
            if not isinstance(value, (float, int, long)):
                continue
            collectd.Values(plugin="ceph.pool.%s" % pool['pool_name'],
                            type='gauge',
                            type_instance=key,
                            values=[value]
                            ).dispatch()

        for osd_id in pool['OSDs']:
            osd = filter(lambda x: x['osd'] == osd_id, osds)[0]
            for key, value in osd.iteritems():
                if not isinstance(value, (float, int, long)):
                    continue
                collectd.Values(plugin="ceph.pool.%s.osds.%i" % (pool['pool_name'], osd_id),
                                type='gauge',
                                type_instance=key,
                                values=[value]
                                ).dispatch()


def cephtool_read(data=None):
    osd_json = cephtool_get_json(["osd", "dump"])
    pg_json = cephtool_get_json(["pg", "dump"])
    mon_json = cephtool_get_json(["mon", "dump"])
    df_json = cephtool_get_json(["df"])

    cephtool_read_pools(osd_json, pg_json, df_json)

    collectd.Values(plugin="ceph.osd",\
        type='gauge',\
        type_instance='num_osds',\
        values=[len(osd_json["osds"])]\
    ).dispatch()
    cephtool_read_osd(osd_json["osds"])

    collectd.Values(plugin="ceph.osd",\
        type="gauge",\
        type_instance='kb_used',\
        values=[pg_json["osd_stats_sum"]["kb_used"]]\
    ).dispatch()
    collectd.Values(plugin="ceph.osd",\
        type="gauge",\
        type_instance='kb_avail',\
        values=[pg_json["osd_stats_sum"]["kb_avail"]]\
    ).dispatch()
    collectd.Values(plugin="ceph.osd",\
        type="gauge",\
        type_instance='snap_trim_queue_len',\
        values=[pg_json["osd_stats_sum"]["snap_trim_queue_len"]]\
    ).dispatch()
    collectd.Values(plugin="ceph.osd",\
        type="gauge",\
        type_instance='num_snap_trimming',\
        values=[pg_json["osd_stats_sum"]["num_snap_trimming"]]\
    ).dispatch()

    collectd.Values(plugin="ceph.pg",\
        type="gauge",\
        type_instance='num_pgs',\
        values=[len(pg_json["pg_stats"])]\
    ).dispatch()
    collectd.Values(plugin="ceph.pg",\
        type="gauge",\
        type_instance='num_pgs_per_osd',\
        values=[len(pg_json["pg_stats"]) / len(osd_json["osds"])]\
    ).dispatch()

    cephtool_read_pg_states(pg_json["pg_stats"])

    collectd.Values(plugin="ceph.pg",\
        type="gauge",\
        type_instance='num_pools',\
        values=[len(pg_json["pool_stats"])]\
    ).dispatch()
    collectd.Values(plugin="ceph.pg",\
        type="gauge",\
        type_instance='num_objects',\
        values=[pg_json["pg_stats_sum"]["stat_sum"]["num_objects"]]\
    ).dispatch()
    collectd.Values(plugin="ceph.pg",\
        type="gauge",\
        type_instance='num_bytes',\
        values=[pg_json["pg_stats_sum"]["stat_sum"]["num_bytes"]]\
    ).dispatch()
    collectd.Values(plugin="ceph.pg",\
        type="gauge",\
        type_instance='num_objects_missing_on_primary',\
        values=[pg_json["pg_stats_sum"]["stat_sum"]["num_objects_missing_on_primary"]]\
    ).dispatch()
    collectd.Values(plugin="ceph.pg",\
        type="gauge",\
        type_instance='num_objects_degraded',\
        values=[pg_json["pg_stats_sum"]["stat_sum"]["num_objects_degraded"]]\
    ).dispatch()
    collectd.Values(plugin="ceph.pg",\
        type="gauge",\
        type_instance='num_objects_unfound',\
        values=[pg_json["pg_stats_sum"]["stat_sum"]["num_objects_unfound"]]\
    ).dispatch()

    collectd.Values(plugin="ceph.mon",\
        type="gauge",\
        type_instance='num_mons',\
        values=[len(mon_json["mons"])],
    ).dispatch()
    collectd.Values(plugin="ceph.mon",\
        type="gauge",\
        type_instance='num_mons_in_quorum',\
        values=[len(mon_json["quorum"])],
    ).dispatch()

collectd.register_config(cephtool_config)
collectd.warning("Initializing cephtool plugin")
collectd.register_read(cephtool_read)
