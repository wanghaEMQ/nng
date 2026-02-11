//
// Copyright 2025 Staysail Systems, Inc. <info@staysail.tech>
// Copyright 2018 Capitar IT Group BV <info@capitar.com>
//
// This software is supplied under the terms of the MIT License, a
// copy of which should be located in the distribution where this
// file was obtained (LICENSE.txt).  A copy of the license may also be
// found online at https://opensource.org/licenses/MIT.
//

#include "../../../core/aio.h"
#include "../../../core/defs.h"
#include "../../../core/lmq.h"
#include "../../../core/message.h"
#include "../../../core/pipe.h"
#include "../../../core/pollable.h"
#include "../../../core/protocol.h"

// Bus1 protocol.  The BUS1 protocol is an enhanced version of BUS0 that
// supports both broadcast (send to all peers) and unicast (send to a specific
// peer). Each peer sends a message to its peers. However, bus protocols do not
// "forward" (absent a device). So in order for each participant to receive the
// message in broadcast mode, each sender must be connected to every other node
// in the network (full mesh).
//
// Unicast mode allows targeting a specific pipe by setting the pipe ID on the
// message using nng_msg_set_pipe(). If no pipe ID is set (zero), the message
// is broadcast to all peers (default behavior, same as BUS0).

#ifndef NNI_PROTO_BUS_V1
#define NNI_PROTO_BUS_V1 NNI_PROTO(7, 1)
#endif

typedef struct bus1_pipe bus1_pipe;
typedef struct bus1_sock bus1_sock;

static void bus1_sock_send(void *, nni_aio *);
static void bus1_sock_recv(void *, nni_aio *);

static void bus1_pipe_recv(bus1_pipe *);

static void bus1_pipe_send_cb(void *);
static void bus1_pipe_recv_cb(void *);

// bus1_sock is our per-socket protocol private structure.
struct bus1_sock {
	nni_list     pipes;
	nni_mtx      mtx;
	nni_pollable can_send;
	nni_pollable can_recv;
	nni_lmq      recv_msgs;
	nni_list     recv_wait;
	int          send_buf;
	bool         raw;
};

// bus1_pipe is our per-pipe protocol private structure.
struct bus1_pipe {
	nni_pipe     *pipe;
	bus1_sock    *bus;
	nni_lmq       send_queue;
	nni_list_node node;
	bool          busy;
	nni_aio       aio_recv;
	nni_aio       aio_send;
};

static void
bus1_sock_fini(void *arg)
{
	bus1_sock *s = arg;

	nni_mtx_fini(&s->mtx);
	nni_pollable_fini(&s->can_send);
	nni_pollable_fini(&s->can_recv);
	nni_lmq_fini(&s->recv_msgs);
}

static void
bus1_sock_init(void *arg, nni_sock *ns)
{
	bus1_sock *s = arg;

	NNI_ARG_UNUSED(ns);

	NNI_LIST_INIT(&s->pipes, bus1_pipe, node);
	nni_mtx_init(&s->mtx);
	nni_aio_list_init(&s->recv_wait);
	nni_pollable_init(&s->can_send);
	nni_pollable_init(&s->can_recv);
	nni_lmq_init(&s->recv_msgs, 16);
	s->send_buf = 16;

	s->raw = false;
}

static void
bus1_sock_init_raw(void *arg, nni_sock *ns)
{
	bus1_sock *s = arg;

	bus1_sock_init(arg, ns);
	s->raw = true;
}

static void
bus1_sock_open(void *arg)
{
	NNI_ARG_UNUSED(arg);
}

static void
bus1_sock_close(void *arg)
{
	bus1_sock *s = arg;
	nni_aio   *aio;

	nni_mtx_lock(&s->mtx);
	while ((aio = nni_list_first(&s->recv_wait)) != NULL) {
		nni_list_remove(&s->recv_wait, aio);
		nni_aio_finish_error(aio, NNG_ECLOSED);
	}
	nni_mtx_unlock(&s->mtx);
}

static void
bus1_pipe_stop(void *arg)
{
	bus1_pipe *p = arg;

	nni_aio_stop(&p->aio_send);
	nni_aio_stop(&p->aio_recv);
}

static void
bus1_pipe_fini(void *arg)
{
	bus1_pipe *p = arg;

	nni_aio_fini(&p->aio_send);
	nni_aio_fini(&p->aio_recv);
	nni_lmq_fini(&p->send_queue);
}

static int
bus1_pipe_init(void *arg, nni_pipe *np, void *s)
{
	bus1_pipe *p = arg;

	p->pipe = np;
	p->bus  = s;
	NNI_LIST_NODE_INIT(&p->node);
	nni_aio_init(&p->aio_send, bus1_pipe_send_cb, p);
	nni_aio_init(&p->aio_recv, bus1_pipe_recv_cb, p);
	nni_lmq_init(&p->send_queue, p->bus->send_buf);

	return (0);
}

static int
bus1_pipe_start(void *arg)
{
	bus1_pipe *p = arg;
	bus1_sock *s = p->bus;

	if (nni_pipe_peer(p->pipe) != NNI_PROTO_BUS_V1) {
		nng_log_warn("NNG-PEER-MISMATCH",
		    "Peer pipe protocol %d is not BUS1 protocol, rejected.",
		    nni_pipe_peer(p->pipe));
		return (NNG_EPROTO);
	}

	nni_mtx_lock(&s->mtx);
	nni_list_append(&s->pipes, p);
	nni_mtx_unlock(&s->mtx);

	bus1_pipe_recv(p);

	return (0);
}

static void
bus1_pipe_close(void *arg)
{
	bus1_pipe *p = arg;
	bus1_sock *s = p->bus;

	nni_aio_close(&p->aio_send);
	nni_aio_close(&p->aio_recv);

	nni_mtx_lock(&s->mtx);
	nni_lmq_flush(&p->send_queue);
	if (nni_list_active(&s->pipes, p)) {
		nni_list_remove(&s->pipes, p);
	}
	nni_mtx_unlock(&s->mtx);
}

static void
bus1_pipe_send_cb(void *arg)
{
	bus1_pipe *p = arg;
	bus1_sock *s = p->bus;
	nni_msg   *msg;

	if (nni_aio_result(&p->aio_send) != 0) {
		// closed?
		nni_msg_free(nni_aio_get_msg(&p->aio_send));
		nni_aio_set_msg(&p->aio_send, NULL);
		nni_pipe_close(p->pipe);
		return;
	}

	nni_mtx_lock(&s->mtx);
	if (nni_lmq_get(&p->send_queue, &msg) == 0) {
		nni_aio_set_msg(&p->aio_send, msg);
		nni_pipe_send(p->pipe, &p->aio_send);
	} else {
		p->busy = false;
	}
	nni_mtx_unlock(&s->mtx);
}

static void
bus1_pipe_recv_cb(void *arg)
{
	bus1_pipe *p   = arg;
	bus1_sock *s   = p->bus;
	nni_aio   *aio = NULL;
	nni_msg   *msg;

	if (nni_aio_result(&p->aio_recv) != 0) {
		nni_pipe_close(p->pipe);
		return;
	}

	msg = nni_aio_get_msg(&p->aio_recv);
	nni_aio_set_msg(&p->aio_recv, NULL);
	nni_msg_set_pipe(msg, nni_pipe_id(p->pipe));

	nni_mtx_lock(&s->mtx);
	if (s->raw) {
		nni_msg_header_append_u32(msg, nni_pipe_id(p->pipe));
	}

	if (!nni_list_empty(&s->recv_wait)) {
		aio = nni_list_first(&s->recv_wait);
		nni_aio_list_remove(aio);
		nni_aio_set_msg(aio, msg);
	} else if (nni_lmq_put(&s->recv_msgs, msg) == 0) {
		nni_pollable_raise(&s->can_recv);
	} else {
		// dropped message due to no room
		nni_msg_free(msg);
	}
	nni_mtx_unlock(&s->mtx);

	if (aio != NULL) {
		nni_aio_finish_sync(aio, 0, nni_msg_len(msg));
	}
	bus1_pipe_recv(p);
}

static void
bus1_pipe_recv(bus1_pipe *p)
{
	nni_pipe_recv(p->pipe, &p->aio_recv);
}

static void
bus1_sock_send(void *arg, nni_aio *aio)
{
	bus1_sock *s = arg;
	nni_msg   *msg;
	bus1_pipe *pipe;
	uint32_t   target_pipe_id = 0;
	uint32_t   sender = 0;
	size_t     len;
	bool       unicast = false;

	msg = nni_aio_get_msg(aio);
	len = nni_msg_len(msg);

	// Check if the message has a target pipe set (for unicast)
	target_pipe_id = nni_msg_get_pipe(msg);
	if (target_pipe_id != 0) {
		unicast = true;
	}

	nni_aio_set_msg(aio, NULL);

	// this test is so that we detect when the aio itself is terminated,
	// otherwise we could loop forever.

	if (s->raw) {
		// In raw mode, we look for the message header, to see if it
		// is being resent from another pipe (e.g. via a device).
		// We don't want to send it back to the originator.
		if (nni_msg_header_len(msg) >= sizeof(uint32_t)) {
			sender = nni_msg_header_trim_u32(msg);
		}
	} else {
		// In cooked mode just strip the header.
		nni_msg_header_clear(msg);
	}

	nni_mtx_lock(&s->mtx);

	if (!nni_aio_start(aio, NULL, NULL)) {
		nni_mtx_unlock(&s->mtx);
		return;
	}

	if (unicast) {
		// Unicast mode: send only to the specified pipe
		NNI_LIST_FOREACH (&s->pipes, pipe) {
			if (nni_pipe_id(pipe->pipe) == target_pipe_id) {
				// Found the target pipe
				if (s->raw && target_pipe_id == sender) {
					// Don't echo back in raw mode
					break;
				}

				if (!pipe->busy) {
					pipe->busy = true;
					nni_aio_set_msg(&pipe->aio_send, msg);
					nni_pipe_send(pipe->pipe, &pipe->aio_send);
					msg = NULL; // Ownership transferred
				} else if (!nni_lmq_full(&pipe->send_queue)) {
					nni_lmq_put(&pipe->send_queue, msg);
					msg = NULL; // Ownership transferred
				}
				break;
			}
		}

		// If msg is still non-NULL, target pipe not found or queue full
		if (msg != NULL) {
			nni_msg_free(msg);
		}
	} else {
		// Broadcast mode: send to all pipes (same as bus0)
		NNI_LIST_FOREACH (&s->pipes, pipe) {

			if (s->raw && nni_pipe_id(pipe->pipe) == sender) {
				continue;
			}

			// if the pipe isn't busy, then send this message direct.
			if (!pipe->busy) {
				pipe->busy = true;
				nni_msg_clone(msg);
				nni_aio_set_msg(&pipe->aio_send, msg);
				nni_pipe_send(pipe->pipe, &pipe->aio_send);
			} else if (!nni_lmq_full(&pipe->send_queue)) {
				nni_msg_clone(msg);
				nni_lmq_put(&pipe->send_queue, msg);
			}
		}
		nni_msg_free(msg);
	}
	nni_mtx_unlock(&s->mtx);

	nni_aio_finish(aio, 0, len);
}

static void
bus1_recv_cancel(nng_aio *aio, void *arg, nng_err rv)
{
	bus1_sock *s = arg;
	nni_mtx_lock(&s->mtx);
	if (nni_aio_list_active(aio)) {
		nni_aio_list_remove(aio);
		nni_aio_finish_error(aio, rv);
	}
	nni_mtx_unlock(&s->mtx);
}

static void
bus1_sock_recv(void *arg, nni_aio *aio)
{
	bus1_sock *s = arg;
	nni_msg   *msg;

	nni_mtx_lock(&s->mtx);
again:
	if (nni_lmq_empty(&s->recv_msgs)) {
		if (!nni_aio_start(aio, bus1_recv_cancel, s)) {
			nni_mtx_unlock(&s->mtx);
			return;
		}
		nni_list_append(&s->recv_wait, aio);
		nni_mtx_unlock(&s->mtx);
		return;
	}

	(void) nni_lmq_get(&s->recv_msgs, &msg);

	if (nni_lmq_empty(&s->recv_msgs)) {
		nni_pollable_clear(&s->can_recv);
	}
	if ((msg = nni_msg_unique(msg)) == NULL) {
		goto again;
	}
	nni_aio_set_msg(aio, msg);
	nni_mtx_unlock(&s->mtx);
	nni_aio_finish(aio, 0, nni_msg_len(msg));
}

static nng_err
bus1_sock_get_send_fd(void *arg, int *fdp)
{
	bus1_sock *sock = arg;
	// BUS sockets are *always* writable (best effort)
	nni_pollable_raise(&sock->can_send);
	return (nni_pollable_getfd(&sock->can_send, fdp));
}

static nng_err
bus1_sock_get_recv_fd(void *arg, int *fdp)
{
	bus1_sock *s = arg;

	return (nni_pollable_getfd(&s->can_recv, fdp));
}

static nng_err
bus1_sock_get_recv_buf_len(void *arg, void *buf, size_t *szp, nni_type t)
{
	bus1_sock *s = arg;
	int        val;
	nni_mtx_lock(&s->mtx);
	val = (int) nni_lmq_cap(&s->recv_msgs);
	nni_mtx_unlock(&s->mtx);

	return (nni_copyout_int(val, buf, szp, t));
}

static nng_err
bus1_sock_get_send_buf_len(void *arg, void *buf, size_t *szp, nni_type t)
{
	bus1_sock *s = arg;
	int        val;
	nni_mtx_lock(&s->mtx);
	val = s->send_buf;
	nni_mtx_unlock(&s->mtx);
	return (nni_copyout_int(val, buf, szp, t));
}

static nng_err
bus1_sock_set_recv_buf_len(void *arg, const void *buf, size_t sz, nni_type t)
{
	bus1_sock *s = arg;
	int        val;
	nng_err    rv;

	if ((rv = nni_copyin_int(&val, buf, sz, 1, 8192, t)) != NNG_OK) {
		return (rv);
	}
	nni_mtx_lock(&s->mtx);
	if ((rv = nni_lmq_resize(&s->recv_msgs, (size_t) val)) != 0) {
		nni_mtx_unlock(&s->mtx);
		return (rv);
	}

	nni_mtx_unlock(&s->mtx);
	return (NNG_OK);
}

static nng_err
bus1_sock_set_send_buf_len(void *arg, const void *buf, size_t sz, nni_type t)
{
	bus1_sock *s = arg;
	bus1_pipe *p;
	int        val;
	nng_err    rv;

	if ((rv = nni_copyin_int(&val, buf, sz, 1, 8192, t)) != NNG_OK) {
		return (rv);
	}

	nni_mtx_lock(&s->mtx);
	s->send_buf = val;
	NNI_LIST_FOREACH (&s->pipes, p) {
		// If we fail part way through (should only be ENOMEM), we
		// stop short.  The others would likely fail for ENOMEM as
		// well anyway.  There is a weird effect here where the
		// buffers may have been set for *some* of the pipes, but
		// we have no way to correct partial failure.
		if ((rv = nni_lmq_resize(&p->send_queue, (size_t) val)) != 0) {
			break;
		}
	}
	nni_mtx_unlock(&s->mtx);
	return (rv);
}

static nni_proto_pipe_ops bus1_pipe_ops = {
	.pipe_size  = sizeof(bus1_pipe),
	.pipe_init  = bus1_pipe_init,
	.pipe_fini  = bus1_pipe_fini,
	.pipe_start = bus1_pipe_start,
	.pipe_close = bus1_pipe_close,
	.pipe_stop  = bus1_pipe_stop,
};

static nni_option bus1_sock_options[] = {
	{
	    .o_name = NNG_OPT_RECVBUF,
	    .o_get  = bus1_sock_get_recv_buf_len,
	    .o_set  = bus1_sock_set_recv_buf_len,
	},
	{
	    .o_name = NNG_OPT_SENDBUF,
	    .o_get  = bus1_sock_get_send_buf_len,
	    .o_set  = bus1_sock_set_send_buf_len,
	},
	// terminate list
	{
	    .o_name = NULL,
	},
};

static nni_proto_sock_ops bus1_sock_ops = {
	.sock_size         = sizeof(bus1_sock),
	.sock_init         = bus1_sock_init,
	.sock_fini         = bus1_sock_fini,
	.sock_open         = bus1_sock_open,
	.sock_close        = bus1_sock_close,
	.sock_send         = bus1_sock_send,
	.sock_recv         = bus1_sock_recv,
	.sock_send_poll_fd = bus1_sock_get_send_fd,
	.sock_recv_poll_fd = bus1_sock_get_recv_fd,
	.sock_options      = bus1_sock_options,
};

static nni_proto_sock_ops bus1_sock_ops_raw = {
	.sock_size         = sizeof(bus1_sock),
	.sock_init         = bus1_sock_init_raw,
	.sock_fini         = bus1_sock_fini,
	.sock_open         = bus1_sock_open,
	.sock_close        = bus1_sock_close,
	.sock_send         = bus1_sock_send,
	.sock_recv         = bus1_sock_recv,
	.sock_send_poll_fd = bus1_sock_get_send_fd,
	.sock_recv_poll_fd = bus1_sock_get_recv_fd,
	.sock_options      = bus1_sock_options,
};

static nni_proto bus1_proto = {
	.proto_self     = { NNI_PROTO_BUS_V1, "bus" },
	.proto_peer     = { NNI_PROTO_BUS_V1, "bus" },
	.proto_flags    = NNI_PROTO_FLAG_SNDRCV,
	.proto_sock_ops = &bus1_sock_ops,
	.proto_pipe_ops = &bus1_pipe_ops,
};

static nni_proto bus1_proto_raw = {
	.proto_self     = { NNI_PROTO_BUS_V1, "bus" },
	.proto_peer     = { NNI_PROTO_BUS_V1, "bus" },
	.proto_flags    = NNI_PROTO_FLAG_SNDRCV | NNI_PROTO_FLAG_RAW,
	.proto_sock_ops = &bus1_sock_ops_raw,
	.proto_pipe_ops = &bus1_pipe_ops,
};

int
nng_bus1_open(nng_socket *id)
{
	return (nni_proto_open(id, &bus1_proto));
}

int
nng_bus1_open_raw(nng_socket *id)
{
	return (nni_proto_open(id, &bus1_proto_raw));
}
