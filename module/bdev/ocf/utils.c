/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "spdk/stdinc.h"

#include "utils.h"
#include "vbdev_ocf.h"

static char *cache_modes[ocf_cache_mode_max] = {
	[ocf_cache_mode_wt] = "wt",
	[ocf_cache_mode_wb] = "wb",
	[ocf_cache_mode_wa] = "wa",
	[ocf_cache_mode_pt] = "pt",
	[ocf_cache_mode_wi] = "wi",
	[ocf_cache_mode_wo] = "wo",
};

ocf_cache_mode_t
ocf_get_cache_mode(const char *cache_mode)
{
	int i;

	for (i = 0; i < ocf_cache_mode_max; i++) {
		if (strcmp(cache_mode, cache_modes[i]) == 0) {
			return i;
		}
	}

	return ocf_cache_mode_none;
}

const char *
ocf_get_cache_modename(ocf_cache_mode_t mode)
{
	if (mode > ocf_cache_mode_none && mode < ocf_cache_mode_max) {
		return cache_modes[mode];
	} else {
		return NULL;
	}
}

static int
mngt_poll_fn(void *opaque)
{
	struct vbdev_ocf *vbdev = opaque;

	if (vbdev->mngt_ctx.poller_fn) {
		if (vbdev->mngt_ctx.timeout_ts &&
		    spdk_get_ticks() >= vbdev->mngt_ctx.timeout_ts) {
			vbdev_ocf_mngt_continue(vbdev, -ETIMEDOUT);
		} else {
			vbdev->mngt_ctx.poller_fn(vbdev);
		}
	}

	return 0;
}

int
vbdev_ocf_mngt_start(struct vbdev_ocf *vbdev, vbdev_ocf_mngt_fn *path,
		     vbdev_ocf_mngt_callback cb, void *cb_arg)
{
	if (vbdev->mngt_ctx.current_step) {
		return -EBUSY;
	}

	memset(&vbdev->mngt_ctx, 0, sizeof(vbdev->mngt_ctx));

	vbdev->mngt_ctx.poller = spdk_poller_register(mngt_poll_fn, vbdev, 200);
	if (vbdev->mngt_ctx.poller == NULL) {
		return -ENOMEM;
	}

	vbdev->mngt_ctx.current_step = path;
	vbdev->mngt_ctx.cb = cb;
	vbdev->mngt_ctx.cb_arg = cb_arg;

	(*vbdev->mngt_ctx.current_step)(vbdev);

	return 0;
}


static void
vbdev_ocf_mngt_poll_set_timeout(struct vbdev_ocf *vbdev, uint64_t millisec)
{
	uint64_t ticks;

	ticks = millisec * spdk_get_ticks_hz() / 1000;
	vbdev->mngt_ctx.timeout_ts = spdk_get_ticks() + ticks;
}

void
vbdev_ocf_mngt_poll(struct vbdev_ocf *vbdev, vbdev_ocf_mngt_fn fn)
{
	assert(vbdev->mngt_ctx.poller != NULL);
	vbdev->mngt_ctx.poller_fn = fn;
	vbdev_ocf_mngt_poll_set_timeout(vbdev, 5000);
}

void
vbdev_ocf_mngt_stop(struct vbdev_ocf *vbdev, vbdev_ocf_mngt_fn *rollback_path, int status)
{
	if (status) {
		vbdev->mngt_ctx.status = status;
	}

	if (vbdev->mngt_ctx.status && rollback_path) {
		vbdev->mngt_ctx.poller_fn = NULL;
		vbdev->mngt_ctx.current_step = rollback_path;
		(*vbdev->mngt_ctx.current_step)(vbdev);
		return;
	}

	spdk_poller_unregister(&vbdev->mngt_ctx.poller);

	if (vbdev->mngt_ctx.cb) {
		vbdev->mngt_ctx.cb(vbdev->mngt_ctx.status, vbdev, vbdev->mngt_ctx.cb_arg);
	}

	memset(&vbdev->mngt_ctx, 0, sizeof(vbdev->mngt_ctx));
}

void
vbdev_ocf_mngt_continue(struct vbdev_ocf *vbdev, int status)
{
	if (vbdev->mngt_ctx.current_step == NULL) {
		return;
	}

	assert((*vbdev->mngt_ctx.current_step) != NULL);

	vbdev->mngt_ctx.status = status;
	vbdev->mngt_ctx.poller_fn = NULL;

	vbdev->mngt_ctx.current_step++;
	if (*vbdev->mngt_ctx.current_step) {
		(*vbdev->mngt_ctx.current_step)(vbdev);
		return;
	}

	vbdev_ocf_mngt_stop(vbdev, NULL, 0);
}

int
vbdev_ocf_mngt_get_status(struct vbdev_ocf *vbdev)
{
	return vbdev->mngt_ctx.status;
}
