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

#define RETRY_ON_EINTR(ret, expr) \
	while(1) { \
		if (expr < 0) { \
			if (errno == EINTR) \
				continue; \
			ret = errno; \
			break; \
		} \
		ret = 0; \
		break; \
	}

/** Polling interval in seconds */
#define CEPH_POLLING_INTERVAL 5

/** Timeout interval in seconds */
#define CEPH_TIMEOUT_INTERVAL 2

/** Maximum length of a daemon name */
#define CEPH_DAEMON_NAME_MAX 64

/** Maximum path length for a UNIX domain socket on this system */
#define UNIX_DOMAIN_SOCK_PATH_MAX (sizeof(((struct sockaddr_un*)0)->sun_path))

enum cstate_t {
	CSTATE_UNCONNECTED = 0,
	CSTATE_WRITE_REQUEST,
	CSTATE_READ_AMT,
	CSTATE_READ_JSON,
	CSTATE_DONE,
};

enum ceph_io_action_t {
	CIO_ACTION_DEFINE_SCHEMA,
	CIO_ACTION_REPORT_DATA,
}

/** Represents a Ceph daemon */
struct ceph_daemon
{
	/** Daemon name, like osd0 or mon.a */
	char name[CEPH_DAEMON_NAME_MAX];

	/** Path to the socket that we use to talk to the ceph daemon */
	char asok_path[UNIX_DOMAIN_SOCK_PATH_MAX];

	/** Connection state */
	enum cstate_t state;

	/** The socket we use to talk to this daemon */ 
	int asok;

	/** The amount of data remaining to read / write. */
	uint32_t amt;

	/** Length of the JSON to read */
	uint32_t json_len;

	/** Buffer containing JSON data */
	char *json;

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

/** Wipe daemon I/O state */
static void daemon_wipe_io_state(struct ceph_daemon *d, enum cstate_t state)
{
	d->state = state;
	if (d->asok != -1) {
		int res;
		RETRY_ON_EINTR(res, close(d->asok));
	}
	d->asok = -1;
	d->amt = 0;
	d->json_len = 0;
	sfree(d->json);
	d->json = NULL;
}

static int define_schema(const char *name, json_object *jo)
{

}

/** Depth-first traversal of the JSON parse tree.
 */
static int traverse_json(char *prefix, int max_prefix, json_object *jo,
			 enum ceph_io_action_t act)
{
	struct json_object_iter iter;
	int ret, plen, klen;

	plen = strlen(prefix);
	json_object_object_foreachC(jo, iter) {
		klen = strlen(iter.key);
		if (plen + klen + 2 > max_prefix)
			return -ENAMETOOLONG;
		strcat(prefix, ".");
		strcat(prefix, iter.key);
		switch (act) {
		case CIO_ACTION_DEFINE_SCHEMA
			ret = define_schema(prefix, iter.val);
			if (ret)
				return ret;
			break;
		case CIO_ACTION_REPORT_DATA
			report_data(prefix, iter.val);
			break;
		default:
			break;
		}

json_object_get_type(iter.val)
		switch (json_object_get_type(iter.val)) {
		case json_type_boolean:
		case json_type_double:
		case json_type_int:
		case json_type_string: {
			const char *vstr = json_object_get_string(iter.val);
			set_kv(prefix, vstr);
			break;
		}
		case json_type_object:
			ret = flatten_json_impl(prefix, max_prefix, iter.val);
			if (ret)
				return ret;
			break;
		case json_type_array:
			return -ENOTSUP;
		}
		prefix[plen] = '\0';
	}
	return 0;
}

static int process_json(const struct ceph_daemon *d, enum ceph_io_action_t act)
{
	json_object *root;
	char buf[128];
	snprintf(buf, sizeof(buf), "%s", d->name);

	root = json_tokener_parse(d->json_buf);
	if (!root)
		return -EDOM;
	return traverse_json(buf, sizeof(buf), root);
}

/** Returns the difference between two struct timevals in milliseconds.
 * On overflow, we return max/min int.
 */
static int milli_diff(const struct timeval *t1, const struct timeval *t2)
{
	int64_t ret;
	int sec_diff = t1.tv_sec - t2.tv_sec;
	int usec_diff = t1.tv_usec - t2.tv_usec;
	ret = usec_diff / 1000;
	ret += (sec_diff * 1000);
	if (ret > MAX_INT)
		return MAX_INT;
	else if (ret < MIN_INT)
		return MIN_INT;
	return (int)ret;
}

/** Ceph configuration callback.
 *
 * This will be called once for each <Plugin /> block parsed by this plugin. 
 * Logically, each Plugin section represents a single Ceph daemon.
 */
static int ceph_config(oconfig_item_t *ci)
{
	int i;
	struct ceph_daemon *array, *nd, cd;
	memset(&cd, sizeof(cd), 0);

	for (i = 0; i < ci->children_num; ++i) {
		oconfig_item_t *child = ci->children + i;
		if (strcasecmp("Name", child->key) == 0) {
			if (snprintf(cd->name, sizeof(cd->name), "%s",
				     child->value) > (sizeof(cd->name) - 1)) {
				ERROR("ceph plugin: daemon name '%s' is too long.\n",
				      child->value);
				return -ENAMETOOLONG;
			}
		}
		else if (strcasecmp("SocketPath", child->key) == 0) {
			if (snprintf(cd->asok_path, sizeof(cd->asok_path), "%s",
				     child->value) > (sizeof(cd->asok_path) - 1)) {
				ERROR("ceph plugin: socket path '%s' is too long.\n",
				      child->value);
				return -ENAMETOOLONG;
			}
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
	g_daemons = array;
	nd = malloc(sizeof(struct ceph_daemon));
	if (!nd)
		return ENOMEM;
	memcpy(nd, cd, sizeof(cd));
	g_daemons[g_num_daemons++] = nd;
	return 0;
}

/** Create a nonblocking connection to a unix domain socket with the given path */
static int asok_connect(const char *path)
{
	int fd = socket(PF_UNIX, SOCK_STREAM, 0);
	if (fd < 0) {
		int err = errno;
		ERROR("socket(PF_UNIX, SOCK_STREAM, 0) failed: error %d", err);
		return -1;
	}
	struct sockaddr_un address;
	memset(&address, 0, sizeof(struct sockaddr_un));
	address.sun_family = AF_UNIX;
	snprintf(address.sun_path, sizeof(address.sun_path), "%s", path);
	if (connect(fd, (struct sockaddr *) &address, 
			sizeof(struct sockaddr_un)) != 0) {
		int err = errno;
		ERROR("connect(%d) failed: error %d", fd, err);
		return -1;
	}

	flags = fcntl(fd, F_GETFL, 0);
	if (fcntl(fd, F_GETFL, flags | O_NONBLOCK) != 0) {
		ERROR("fcntl(%d, O_NONBLOCK) error", fd, err);
		return -1;
	}
	return fd;
}

/** Shut down the Ceph plugin */
static int ceph_shutdown(void)
{
	int i;
	for (i = 0; i < g_num_daemons; ++i) {
		struct ceph_daemon *d = g_daemons + i;
		daemon_wipe_io_state(d, CSTATE_DONE);
		sfree(d);
	}
	sfree(g_daemons);
	g_daemons = NULL;
	g_num_daemons = 0;
}

static int ceph_read_setup(struct ceph_daemon *d, struct pollfd* fds)
{
	switch (d->state) {
	case CSTATE_UNCONNECTED:
		d->asok = asok_connect(d->asok_path);
		if (d->asok == -1) {
			return 0;
		}
		fds->fd = d->asok;
		fds->events = POLLOUT | POLLWRBAND;
		fds++;
		return 1;
	case CSTATE_WRITE_REQUEST:
		fds->fd = d->asok;
		fds->events = POLLOUT | POLLWRBAND;
		fds++;
		return 1;
	case CSTATE_READ_AMT:
	case CSTATE_READ_JSON:
		fds->fd = d->asok;
		fds->events = POLLIN | POLLRDBAND;
		fds++;
		return 1;
	case CSTATE_DONE:
		// do nothing
		return 0;
	}
}

static int do_ceph_daemon_io(struct ceph_daemon *d, const struct pollfd *pfd,
			     enum ceph_io_action_t act)
{
	switch (d->state) {
	case CSTATE_UNCONNECTED:
		/* unreachable */
		ERROR("do_ceph_daemon_io(name=%s) got to illegal state on line %d",
		      d->name, __LINE__);
		return;
	case CSTATE_WRITE_REQUEST: {
		int res;
		uint32_t cmd_raw = htonl(0x1);
		RETRY_ON_EINTR(res, write(d->asok, ((char*)cmd) + d->amt,
					       sizeof(cmd_raw) - d->amt));
		if (res == -1)
			return errno;
		d->amt += res;
		if (d->amt >= sizeof(cmd_raw)) {
			d->amt = 0;
			d->state = CSTATE_READ_AMT;
		}
		return 0;
	}
	case CSTATE_READ_AMT: {
		int res;
		RETRY_ON_EINTR(res, read(d->asok, ((char*)d->json_len) + d->amt,
					       sizeof(d->json_len) - d->amt));
		if (res == -1)
			return errno;
		d->amt += res;
		if (d->amt >= sizeof(cmd_raw)) {
			d->json_len = ntohl(d->json_len);
			d->amt = 0;
			d->state = CSTATE_READ_JSON;
			d->json = malloc(d->json_len);
			if (!d->json)
				return -ENOBUFS;
		}
		return 0;
	}
	case CSTATE_READ_JSON: {
		int res;
		RETRY_ON_EINTR(res, read(d->asok, d->json + d->amt,
					      d->json_len - d->amt));
		if (res == -1)
			return errno;
		d->amt += res;
		if (d->amt >= d->json_len) {
			int ret = process_json(d, act);
			if (ret)
				return ret;
			daemon_wipe_io_state(d, CSTATE_DONE);
		}
		return 0;
	}
	case CSTATE_DONE:
		// do nothing
		return 0;
	}
}

/** This handles the actual network I/O to talk to the Ceph daemons.
 *
 * This is called from ceph_init with CIO_ACTION_DEFINE_SCHEMA to figure out
 * what data the daemons are reporting.  This is also called from ceph_read
 * with CIO_ACTION_REPORT_DATA to report the data.
 */
static int do_ceph_io(enum ceph_io_action_t act)
{
	int i, num_read = 0;

	/** Calculate the time at which we should give up */
	struct timeval end_tv;
	gettimeofday(&end_tv, NULL);
	end_tv.tv_sec += CEPH_TIMEOUT_INTERVAL;

	while (true) {
		struct pollfd fds[g_num_daemons];
		struct ceph_daemon *pdaemons[g_num_daemons];
		int nfds, diff;
		struct timeval tv;

		memset(fds, 0, sizeof(fds));
		nfds = 0;
		for (i = 0; i < g_num_daemons; ++i) {
			if (ceph_read_setup(g_daemons + i, fds + nfds)) {
				pdaemons[nfds++] = g_daemons + i;
			}
		}
		if (nfds == 0) {
			/* Success */
			return 0;
		}
		gettimeofday(&tv, NULL);
		diff = milli_diff(end_tv, tv);
		if (diff <= 0) {
			/* Timed out */
			return -ETIMEOUT;
		}
		int ret = poll(fds, nfds, diff);
		if (ret < 0) {
			int err = errno;
			if (err == EINTR)
				continue;
			ERROR("poll(2) error: %d", err);
			return err;
		}
		for (i = 0; i < nfds; ++i) {
			int ret = do_ceph_daemon_io(pdaemons + i, fds + i, act);
			if (ret) {
				ERROR("do_ceph_daemon_io(name=%s) got error %d",
				      pdaemons[i].name, ret);
				daemon_wipe_io_state(pdaemons + i,
						     CSTATE_UNCONNECTED);
			}
		}
	}
}

/** Read data from the Ceph daemons */
static int ceph_read(void)
{
	/* reset all I/O state */
	for (i = 0; i < g_num_daemons; ++i) {
		struct ceph_daemon *d = g_daemons + i;
		if (d->data_set_len == 0)
			daemon_wipe_io_state(g_daemons + i, CSTATE_DONE);
		else
			daemon_wipe_io_state(g_daemons + i, CSTATE_UNCONNECTED);
	}
	return do_ceph_io(CIO_ACTION_REPORT_DATA);
}

static int ceph_read_schema(void)
{
	/* reset all I/O state */
	for (i = 0; i < g_num_daemons; ++i) {
		struct ceph_daemon *d = g_daemons + i;
		daemon_wipe_io_state(g_daemons + i, CSTATE_UNCONNECTED);
	}
	return do_ceph_io(CIO_ACTION_DEFINE_SCHEMA);
}

void module_register(void)
{
	plugin_register_complex_config("ceph", ceph_config);
	plugin_register_shutdown("ceph", ceph_shutdown);
	plugin_register_data_set(&g_data_set);
	plugin_register_read("ceph", ceph_read);
}
