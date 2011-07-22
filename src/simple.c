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

/** Polling interval in seconds */
#define CEPH_POLLING_INTERVAL 5

/** Timeout interval in seconds */
#define CEPH_TIMEOUT_INTERVAL 2

/** Maximum length of a daemon name */
#define CEPH_DAEMON_NAME_MAX 64

/** Maximum path length for a UNIX domain socket on this system */
#define UNIX_DOMAIN_SOCK_PATH_MAX (sizeof(((struct sockaddr_un*)0)->sun_path))

/** Represents a Ceph daemon */
struct ceph_daemon
{
	/** Daemon name, like osd0 or mon.a */
	char name[CEPH_DAEMON_NAME_MAX];

	/** Path to the socket that we use to talk to the ceph daemon */
	char asok_path[UNIX_DOMAIN_SOCK_PATH_MAX];

	/** Number of key/value pairs this daemon reports */
	int data_set_len;

	/** Represents a key/value pair that this daemon reports */
	struct data_set_s *data_sets;
	struct data_source_s *data_sources;
};

/** Array of daemons to monitor */
static struct ceph_daemon **g_daemons = NULL;

/** Number of elements in g_daemons */
static int g_num_daemons = 0;

static void ceph_print_daemon_state(const struct ceph_daemon *d)
{
	WARNING("name=%s, asok_path=%s", d->name, d->asok_path);
}

static void ceph_print_daemons_state(void)
{
	int i;
	for (i = 0; i < g_num_daemons; ++i) {
		ceph_print_daemon_state(g_daemons[i]);
	}
}

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
	memset(&cd, sizeof(cd), 0);

	WARNING("entering ceph_config lol!");

	for (i = 0; i < ci->children_num; ++i) {
		oconfig_item_t *child = ci->children + i;
		if (strcasecmp("Name", child->key) == 0) {
			ret = cc_handle_str(child, cd.name, sizeof(cd.name));
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
	if (cd.name[0] == '\0') {
		ERROR("ceph plugin: you must configure a daemon name.\n");
		return -EINVAL;
	}
	else if (cd.asok_path[0] == '\0') {
		ERROR("ceph plugin(name=%s): you must configure an administrative "
		      "socket path.\n", cd.name);
		return -EINVAL;
	}
	else if (!((cd.asok_path[0] == '/') ||
	      (cd.asok_path[0] == '.' && cd.asok_path[1] == '/'))) {
		ERROR("ceph plugin(name=%s): administrative socket paths must begin with "
		      "'/' or './' Can't parse: '%s'\n",
		      cd.name, cd.asok_path);
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
	memcpy(nd, &cd, sizeof(cd));
	g_daemons[g_num_daemons++] = nd;
	return 0;
}

static int ceph_init(void)
{
	WARNING("ceph_init");
	ceph_print_daemons_state();
	return 0;
}

static int ceph_shutdown(void)
{
	int i;
	for (i = 0; i < g_num_daemons; ++i) {
		struct ceph_daemon *d = g_daemons[i];
		sfree(d);
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
	plugin_register_shutdown("ceph", ceph_shutdown);
}
