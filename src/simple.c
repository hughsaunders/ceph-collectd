/**
 * collectd - src/ceph.c
 * Copyright (C) 2011  New Dream Network
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; only version 2 of the License is applicable.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * Authors:
 *   Colin McCabe <cmccabe@alumni.cmu.edu>
 **/

#define _BSD_SOURCE

#include "collectd.h"
#include "common.h"
#include "plugin.h"

#include <errno.h>
#include <fcntl.h>
#include <json/json.h>
#include <json/json_object_private.h> /* need for struct json_object_iter */
#include <limits.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

static const char *HARDCODED_JSON = 
	"{\"test_perfcounter_1\":{\"element1\":0,\"element2\":0,\"element3\":"
		"{\"count\":0,\"sum\":0}},\"test_perfcounter_2\":{\"foo\":0,\"bar\":0}}";

/** Polling interval in seconds */
#define CEPH_POLLING_INTERVAL 5

/** Timeout interval in seconds */
#define CEPH_TIMEOUT_INTERVAL 2

/** Maximum path length for a UNIX domain socket on this system */
#define UNIX_DOMAIN_SOCK_PATH_MAX (sizeof(((struct sockaddr_un*)0)->sun_path))

/** Represents a Ceph daemon */
struct ceph_daemon
{
	/** Path to the socket that we use to talk to the ceph daemon */
	char asok_path[UNIX_DOMAIN_SOCK_PATH_MAX];

	/** The set of  key/value pairs that this daemon reports
	 * dset.type		The daemon name
	 * dset.ds_num		Number of data sources (key/value pairs) 
	 * dset.ds		Dynamically allocated array of key/value pairs
	 */
	struct data_set_s dset;
};

/** Array of daemons to monitor */
static struct ceph_daemon **g_daemons = NULL;

/** Number of elements in g_daemons */
static int g_num_daemons = 0;

static void ceph_daemon_print(const struct ceph_daemon *d)
{
	WARNING("name=%s, asok_path=%s", d->dset.type, d->asok_path);
}

static void ceph_daemons_print(void)
{
	int i;
	for (i = 0; i < g_num_daemons; ++i) {
		ceph_daemon_print(g_daemons[i]);
	}
}

static void ceph_daemon_free(struct ceph_daemon *d)
{
	plugin_unregister_data_set(d->dset.type);
	sfree(d->dset.ds);
	sfree(d);
}

static int ceph_daemon_add_ds_entry(struct ceph_daemon *d,
				    const char *name, int type)
{
	struct data_source_s *ds;
	if (strlen(name) + 1 > DATA_MAX_NAME_LEN)
		return -ENAMETOOLONG;
	struct data_source_s *ds_array = realloc(d->dset.ds,
		 sizeof(struct data_source_s) * (d->dset.ds_num + 1));
	if (!ds_array)
		return -ENOMEM;
	d->dset.ds = ds_array;
	ds = &ds_array[d->dset.ds_num++];
	snprintf(ds->name, DATA_MAX_NAME_LEN, "%s", name);
	ds->type = type;
	ds->min = NAN;
	ds->max = NAN;
	return 0;
}

/******* ceph_config *******/
static int cc_handle_str(struct oconfig_item_s *item, char *dest, int dest_len)
{
	const char *val;
	if (item->values_num != 1) {
		return -ENOTSUP; 
	}
	if (item->values[0].type != OCONFIG_TYPE_STRING) {
		return -ENOTSUP; 
	}
	val = item->values[0].value.string;
	if (snprintf(dest, dest_len, "%s", val) > (dest_len - 1)) {
		ERROR("ceph plugin: configuration parameter '%s' is too long.\n",
		      item->key);
		return -ENAMETOOLONG;
	}
	return 0;
}

static int ceph_config(oconfig_item_t *ci)
{
	int ret, i;
	struct ceph_daemon *array, *nd, cd;
	memset(&cd, 0, sizeof(struct ceph_daemon));

	WARNING("entering ceph_config lol!");

	for (i = 0; i < ci->children_num; ++i) {
		oconfig_item_t *child = ci->children + i;
		if (strcasecmp("Name", child->key) == 0) {
			ret = cc_handle_str(child, cd.dset.type, DATA_MAX_NAME_LEN);
			if (ret)
				return ret;
		}
		else if (strcasecmp("SocketPath", child->key) == 0) {
			ret = cc_handle_str(child, cd.asok_path, sizeof(cd.asok_path));
			if (ret)
				return ret;
		}
		else {
			WARNING("ceph plugin: ignoring unknown option %s\n",
				child->key);
		}
	}
	if (cd.dset.type[0] == '\0') {
		ERROR("ceph plugin: you must configure a daemon name.\n");
		return -EINVAL;
	}
	else if (cd.asok_path[0] == '\0') {
		ERROR("ceph plugin(name=%s): you must configure an administrative "
		      "socket path.\n", cd.dset.type);
		return -EINVAL;
	}
	else if (!((cd.asok_path[0] == '/') ||
	      (cd.asok_path[0] == '.' && cd.asok_path[1] == '/'))) {
		ERROR("ceph plugin(name=%s): administrative socket paths must begin with "
		      "'/' or './' Can't parse: '%s'\n",
		      cd.dset.type, cd.asok_path);
		return -EINVAL;
	}
	array = realloc(g_daemons,
			sizeof(struct ceph_daemon *) * (g_num_daemons + 1));
	if (array == NULL) {
		/* The positive return value here indicates that this is a
		 * runtime error, not a configuration error.  */
		return ENOMEM;
	}
	g_daemons = (struct ceph_daemon**)array;
	nd = malloc(sizeof(struct ceph_daemon));
	if (!nd)
		return ENOMEM;
	memcpy(nd, &cd, sizeof(struct ceph_daemon));
	g_daemons[g_num_daemons++] = nd;
	return 0;
}

/******* JSON parsing *******/
typedef int (*node_handler_t)(void*, json_object*, const char*);

/** Perform a depth-first traversal of the JSON parse tree,
 * calling node_handler at each leaf node.*/
static int traverse_json(json_object *jo, char *key, int max_key,
			node_handler_t handler, void *handler_arg)
{
	struct json_object_iter iter;
	int ret, plen, klen;

	plen = strlen(key);
	json_object_object_foreachC(jo, iter) {
		klen = strlen(iter.key);
		if (plen + klen + 2 > max_key)
			return -ENAMETOOLONG;
		if (plen != 0)
			strcat(key, ".");
		strcat(key, iter.key);
		switch (json_object_get_type(iter.val)) {
		case json_type_object:
			ret = traverse_json(iter.val, key, max_key,
					    handler, handler_arg);
			break;
		case json_type_array:
			ret = -ENOTSUP;
			break;
		default:
			ret = handler(handler_arg, iter.val, key);
			break;
		}
		if (ret)
			return ret;
		key[plen] = '\0';
	}
	return 0;
}

static int process_json(const char *json,
			node_handler_t handler, void *handler_arg)
{
	json_object *root;
	char buf[128];
	buf[0] = '\0';

	root = json_tokener_parse(json);
	if (!root)
		return -EDOM;
	return traverse_json(root, buf, sizeof(buf),
			     handler, handler_arg);
}

static int node_handler_define_schema(void *arg, json_object *jo,
				      const char *key)
{
	struct ceph_daemon *d = (struct ceph_daemon *)arg;

	switch (json_object_get_type(jo)) {
	case json_type_boolean:
	case json_type_double:
	case json_type_int:
		return ceph_daemon_add_ds_entry(d, key, DS_TYPE_GAUGE);
	default:
		return -ENOTSUP;
	}
}

/** A set of values_t data that we build up in memory while parsing the JSON. */
struct values_tmp {
	struct ceph_daemon *d;
	int values_len;
	value_t values[0];
};

static value_t* get_matching_value(const struct data_set_s *dset,
		   const char *name, value_t *values, int num_values)
{
	int i;
	for (i = 0; i < num_values; ++i) {
		if (strcmp(dset->ds[i].name, name) == 0) {
			return values + i;
		}
	}
	return NULL;
}

static int node_handler_fetch_data(void *arg, json_object *jo,
				      const char *key)
{
	value_t *uv;
	struct values_tmp *vtmp = (struct values_tmp*)arg;

	switch (json_object_get_type(jo)) {
	case json_type_boolean:
	case json_type_double:
	case json_type_int:
		uv = get_matching_value(&vtmp->d->dset, key,
					 vtmp->values, vtmp->values_len);
		uv->gauge = json_object_get_double(jo);
		return 0;
	default:
		return -ENOTSUP;
	}
}

static int ceph_read(void)
{
	struct values_tmp *vt, *vtmp = NULL;
	int i, ret = 0;
	for (i = 0; i < g_num_daemons; ++i) {
		value_list_t vl = VALUE_LIST_INIT;
		struct ceph_daemon *d = g_daemons[i];
		size_t sz = sizeof(struct values_tmp) +
				(sizeof(value_t) * d->dset.ds_num);
		vt = realloc(vtmp, sz); 
		if (!vt) {
			ret = -ENOMEM;
			goto done;
		}
		vtmp = vt;
		memset(vtmp, 0, sz);
		vtmp->d = d;
		vtmp->values_len = d->dset.ds_num;
		ret = process_json(HARDCODED_JSON, node_handler_fetch_data, vtmp);
		if (ret)
			goto done;
		sstrncpy(vl.host, hostname_g, sizeof(vl.host));
		sstrncpy(vl.plugin, "ceph", sizeof(vl.plugin));
		sstrncpy(vl.type, d->dset.type, sizeof(vl.type));
		vl.values = vtmp->values;
		vl.values_len = vtmp->values_len;
		ret = plugin_dispatch_values(&vl);
	}
done:
	free(vtmp);
	return ret;
}

/******* lifecycle *******/
static int ceph_init(void)
{
	int ret, i;
	WARNING("ceph_init");
	ceph_daemons_print();

	/* Register the data set for each Ceph daemon */
	for (i = 0; i < g_num_daemons; ++i) {
		struct ceph_daemon *d = g_daemons[i];
		ret = process_json(HARDCODED_JSON, node_handler_define_schema, d);
		if (ret)
			return ret;
		ret = plugin_register_data_set(&d->dset);
		if (ret)
			return ret;
	}
	return 0;
}

static int ceph_shutdown(void)
{
	int i;
	for (i = 0; i < g_num_daemons; ++i) {
		ceph_daemon_free(g_daemons[i]);
	}
	sfree(g_daemons);
	g_daemons = NULL;
	g_num_daemons = 0;
	WARNING("finished ceph_shutdown");
	return 0;
}

void module_register(void)
{
	plugin_register_complex_config("ceph", ceph_config);
	plugin_register_init("ceph", ceph_init);
	plugin_register_read("ceph", ceph_read);
	plugin_register_shutdown("ceph", ceph_shutdown);
}
