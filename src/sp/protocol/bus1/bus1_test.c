//
// Copyright 2025 Staysail Systems, Inc. <info@staysail.tech>
//
// This software is supplied under the terms of the MIT License, a
// copy of which should be located in the distribution where this
// file was obtained (LICENSE.txt).  A copy of the license may also be
// found online at https://opensource.org/licenses/MIT.
//

#include "../../../testing/nuts.h"

#define SECOND 1000

#define BUS1_SELF 0x71
#define BUS1_PEER 0x71
#define BUS1_SELF_NAME "bus"
#define BUS1_PEER_NAME "bus"

void
test_bus1_identity(void)
{
	nng_socket  s;
	uint16_t    p;
	const char *n;

	NUTS_PASS(nng_bus1_open(&s));
	NUTS_PASS(nng_socket_proto_id(s, &p));
	NUTS_TRUE(p == BUS1_SELF);
	NUTS_PASS(nng_socket_peer_id(s, &p));
	NUTS_TRUE(p == BUS1_PEER);
	NUTS_PASS(nng_socket_proto_name(s, &n));
	NUTS_MATCH(n, BUS1_SELF_NAME);
	NUTS_PASS(nng_socket_peer_name(s, &n));
	NUTS_MATCH(n, BUS1_PEER_NAME);
	NUTS_CLOSE(s);
}

static void
test_bus1_broadcast(void)
{
	nng_socket s1, s2, s3;

	NUTS_PASS(nng_bus1_open(&s1));
	NUTS_PASS(nng_bus1_open(&s2));
	NUTS_PASS(nng_bus1_open(&s3));

	NUTS_PASS(nng_socket_set_ms(s1, NNG_OPT_RECVTIMEO, SECOND));
	NUTS_PASS(nng_socket_set_ms(s2, NNG_OPT_RECVTIMEO, SECOND));
	NUTS_PASS(nng_socket_set_ms(s3, NNG_OPT_RECVTIMEO, SECOND));

	NUTS_MARRY(s1, s2);
	NUTS_MARRY(s1, s3);

	// Test broadcast - no pipe set, should reach all
	NUTS_SEND(s1, "broadcast1");
	NUTS_RECV(s2, "broadcast1");
	NUTS_RECV(s3, "broadcast1");

	NUTS_SEND(s2, "broadcast2");
	NUTS_SEND(s1, "broadcast3");
	NUTS_RECV(s1, "broadcast2");
	NUTS_RECV(s2, "broadcast3");
	NUTS_RECV(s3, "broadcast3");

	NUTS_CLOSE(s1);
	NUTS_CLOSE(s2);
	NUTS_CLOSE(s3);
}

// Structure to capture pipe events
typedef struct {
	nng_socket sock;
	nng_pipe   pipe;
	bool       received;
	nng_mtx   *mtx;
	nng_cv    *cv;
} pipe_event;

static void
pipe_notify_cb(nng_pipe p, nng_pipe_ev ev, void *arg)
{
	pipe_event *pe = arg;
	if (ev == NNG_PIPE_EV_ADD_POST) {
		nng_mtx_lock(pe->mtx);
		pe->pipe     = p;
		pe->received = true;
		nng_cv_wake(pe->cv);
		nng_mtx_unlock(pe->mtx);
	}
}

static void
test_bus1_unicast(void)
{
	nng_socket  s1, s2, s3;
	nng_msg    *msg;
	pipe_event  pe2, pe3;
	nng_pipe    p2, p3;

	NUTS_PASS(nng_bus1_open(&s1));
	NUTS_PASS(nng_bus1_open(&s2));
	NUTS_PASS(nng_bus1_open(&s3));

	NUTS_PASS(nng_socket_set_ms(s1, NNG_OPT_RECVTIMEO, SECOND));
	NUTS_PASS(nng_socket_set_ms(s2, NNG_OPT_RECVTIMEO, SECOND));
	NUTS_PASS(nng_socket_set_ms(s3, NNG_OPT_RECVTIMEO, SECOND));

	// Set up pipe notification to capture pipe IDs
	pe2.received = false;
	pe3.received = false;
	nng_mtx_alloc(&pe2.mtx);
	nng_mtx_alloc(&pe3.mtx);
	nng_cv_alloc(&pe2.cv, pe2.mtx);
	nng_cv_alloc(&pe3.cv, pe3.mtx);

	NUTS_PASS(nng_pipe_notify(s1, NNG_PIPE_EV_ADD_POST, pipe_notify_cb, &pe2));

	NUTS_MARRY(s1, s2);

	// Wait for pipe event
	nng_mtx_lock(pe2.mtx);
	while (!pe2.received) {
		nng_cv_wait(pe2.cv);
	}
	p2 = pe2.pipe;
	nng_mtx_unlock(pe2.mtx);

	// Now connect s3 with a new callback
	pe3.received = false;
	NUTS_PASS(nng_pipe_notify(s1, NNG_PIPE_EV_ADD_POST, pipe_notify_cb, &pe3));

	NUTS_MARRY(s1, s3);

	// Wait for second pipe event
	nng_mtx_lock(pe3.mtx);
	while (!pe3.received) {
		nng_cv_wait(pe3.cv);
	}
	p3 = pe3.pipe;
	nng_mtx_unlock(pe3.mtx);

	// Test unicast to s2
	NUTS_PASS(nng_msg_alloc(&msg, 0));
	NUTS_PASS(nng_msg_append(msg, "unicast_to_s2", 13));
	nng_msg_set_pipe(msg, p2);
	NUTS_PASS(nng_sendmsg(s1, msg, 0));

	// s2 should receive
	NUTS_PASS(nng_recvmsg(s2, &msg, 0));
	NUTS_TRUE(nng_msg_len(msg) == 13);
	NUTS_TRUE(memcmp(nng_msg_body(msg), "unicast_to_s2", 13) == 0);
	nng_msg_free(msg);

	// s3 should timeout
	NUTS_FAIL(nng_recvmsg(s3, &msg, 0), NNG_ETIMEDOUT);

	// Test unicast to s3
	NUTS_PASS(nng_msg_alloc(&msg, 0));
	NUTS_PASS(nng_msg_append(msg, "unicast_to_s3", 13));
	nng_msg_set_pipe(msg, p3);
	NUTS_PASS(nng_sendmsg(s1, msg, 0));

	// s3 should receive
	NUTS_PASS(nng_recvmsg(s3, &msg, 0));
	NUTS_TRUE(nng_msg_len(msg) == 13);
	NUTS_TRUE(memcmp(nng_msg_body(msg), "unicast_to_s3", 13) == 0);
	nng_msg_free(msg);

	// s2 should timeout
	NUTS_FAIL(nng_recvmsg(s2, &msg, 0), NNG_ETIMEDOUT);

	nng_cv_free(pe2.cv);
	nng_cv_free(pe3.cv);
	nng_mtx_free(pe2.mtx);
	nng_mtx_free(pe3.mtx);

	NUTS_CLOSE(s1);
	NUTS_CLOSE(s2);
	NUTS_CLOSE(s3);
}

static void
test_bus1_mixed(void)
{
	nng_socket  s1, s2, s3;
	nng_msg    *msg;
	pipe_event  pe2;
	nng_pipe    p2;

	NUTS_PASS(nng_bus1_open(&s1));
	NUTS_PASS(nng_bus1_open(&s2));
	NUTS_PASS(nng_bus1_open(&s3));

	NUTS_PASS(nng_socket_set_ms(s1, NNG_OPT_RECVTIMEO, SECOND));
	NUTS_PASS(nng_socket_set_ms(s2, NNG_OPT_RECVTIMEO, SECOND));
	NUTS_PASS(nng_socket_set_ms(s3, NNG_OPT_RECVTIMEO, SECOND));

	// Set up pipe notification
	pe2.received = false;
	nng_mtx_alloc(&pe2.mtx);
	nng_cv_alloc(&pe2.cv, pe2.mtx);

	NUTS_PASS(nng_pipe_notify(s1, NNG_PIPE_EV_ADD_POST, pipe_notify_cb, &pe2));

	NUTS_MARRY(s1, s2);
	NUTS_MARRY(s1, s3);

	// Wait for pipe event
	nng_mtx_lock(pe2.mtx);
	while (!pe2.received) {
		nng_cv_wait(pe2.cv);
	}
	p2 = pe2.pipe;
	nng_mtx_unlock(pe2.mtx);

	// Send broadcast
	NUTS_SEND(s1, "broadcast");
	NUTS_RECV(s2, "broadcast");
	NUTS_RECV(s3, "broadcast");

	// Send unicast to s2
	NUTS_PASS(nng_msg_alloc(&msg, 0));
	NUTS_PASS(nng_msg_append(msg, "unicast", 7));
	nng_msg_set_pipe(msg, p2);
	NUTS_PASS(nng_sendmsg(s1, msg, 0));

	// s2 should receive unicast
	NUTS_PASS(nng_recvmsg(s2, &msg, 0));
	NUTS_TRUE(nng_msg_len(msg) == 7);
	nng_msg_free(msg);

	// s3 should timeout
	NUTS_FAIL(nng_recvmsg(s3, &msg, 0), NNG_ETIMEDOUT);

	// Another broadcast
	NUTS_SEND(s1, "broadcast2");
	NUTS_RECV(s2, "broadcast2");
	NUTS_RECV(s3, "broadcast2");

	nng_cv_free(pe2.cv);
	nng_mtx_free(pe2.mtx);

	NUTS_CLOSE(s1);
	NUTS_CLOSE(s2);
	NUTS_CLOSE(s3);
}

static void
test_bus1_device(void)
{
	nng_socket s1, s2, s3;
	nng_socket none = NNG_SOCKET_INITIALIZER;
	nng_aio   *aio;

	NUTS_PASS(nng_bus1_open_raw(&s1));
	NUTS_PASS(nng_bus1_open(&s2));
	NUTS_PASS(nng_bus1_open(&s3));
	NUTS_PASS(nng_aio_alloc(&aio, NULL, NULL));

	NUTS_PASS(nng_socket_set_ms(s1, NNG_OPT_RECVTIMEO, SECOND));
	NUTS_PASS(nng_socket_set_ms(s2, NNG_OPT_RECVTIMEO, SECOND));
	NUTS_PASS(nng_socket_set_ms(s3, NNG_OPT_RECVTIMEO, SECOND));

	NUTS_MARRY(s1, s2);
	NUTS_MARRY(s1, s3);

	nng_device_aio(aio, s1, none);

	NUTS_SEND(s2, "two");
	NUTS_SEND(s3, "three");
	NUTS_RECV(s2, "three");
	NUTS_RECV(s3, "two");

	NUTS_CLOSE(s1);
	NUTS_CLOSE(s2);
	NUTS_CLOSE(s3);

	nng_aio_free(aio);
}

static void
test_bus1_validate_peer(void)
{
	nng_socket      s1, s2;
	nng_stat       *stats;
	const nng_stat *reject;
	char           *addr;

	NUTS_ADDR(addr, "inproc");
	NUTS_PASS(nng_bus1_open(&s1));
	NUTS_PASS(nng_pair0_open(&s2));

	NUTS_PASS(nng_listen(s1, addr, NULL, 0));
	NUTS_PASS(nng_dial(s2, addr, NULL, NNG_FLAG_NONBLOCK));

	NUTS_SLEEP(100);
	NUTS_PASS(nng_stats_get(&stats));

	NUTS_TRUE(stats != NULL);
	NUTS_TRUE((reject = nng_stat_find_socket(stats, s1)) != NULL);
	NUTS_TRUE((reject = nng_stat_find(reject, "reject")) != NULL);

	NUTS_TRUE(nng_stat_type(reject) == NNG_STAT_COUNTER);
	NUTS_TRUE(nng_stat_value(reject) > 0);

	NUTS_CLOSE(s1);
	NUTS_CLOSE(s2);
	nng_stats_free(stats);
}

static void
test_bus1_no_context(void)
{
	nng_socket s;
	nng_ctx    ctx;

	NUTS_PASS(nng_bus1_open(&s));
	NUTS_FAIL(nng_ctx_open(&ctx, s), NNG_ENOTSUP);
	NUTS_CLOSE(s);
}

static void
test_bus1_recv_cancel(void)
{
	nng_socket s1;
	nng_aio   *aio;

	NUTS_PASS(nng_bus1_open(&s1));
	NUTS_PASS(nng_aio_alloc(&aio, NULL, NULL));

	nng_aio_set_timeout(aio, SECOND);
	nng_socket_recv(s1, aio);
	nng_aio_abort(aio, NNG_ECANCELED);

	nng_aio_wait(aio);
	NUTS_FAIL(nng_aio_result(aio), NNG_ECANCELED);
	NUTS_CLOSE(s1);
	nng_aio_free(aio);
}

static void
test_bus1_close_recv_abort(void)
{
	nng_socket s1;
	nng_aio   *aio;

	NUTS_PASS(nng_bus1_open(&s1));
	NUTS_PASS(nng_aio_alloc(&aio, NULL, NULL));

	nng_aio_set_timeout(aio, SECOND);
	nng_socket_recv(s1, aio);
	NUTS_CLOSE(s1);

	nng_aio_wait(aio);
	NUTS_FAIL(nng_aio_result(aio), NNG_ECLOSED);
	nng_aio_free(aio);
}

static void
test_bus1_send_no_pipes(void)
{
	nng_socket s1;

	NUTS_PASS(nng_bus1_open(&s1));
	NUTS_SEND(s1, "DROP1");
	NUTS_SEND(s1, "DROP2");
	NUTS_CLOSE(s1);
}

static void
test_bus1_cooked(void)
{
	nng_socket s;
	bool       b;

	NUTS_PASS(nng_bus1_open(&s));
	NUTS_PASS(nng_socket_raw(s, &b));
	NUTS_TRUE(!b);
	NUTS_CLOSE(s);

	NUTS_PASS(nng_bus1_open_raw(&s));
	NUTS_PASS(nng_socket_raw(s, &b));
	NUTS_TRUE(b);
	NUTS_CLOSE(s);
}

TEST_LIST = {
	{ "bus1 identity", test_bus1_identity },
	{ "bus1 broadcast", test_bus1_broadcast },
	{ "bus1 unicast", test_bus1_unicast },
	{ "bus1 mixed", test_bus1_mixed },
	{ "bus1 device", test_bus1_device },
	{ "bus1 validate peer", test_bus1_validate_peer },
	{ "bus1 no context", test_bus1_no_context },
	{ "bus1 recv cancel", test_bus1_recv_cancel },
	{ "bus1 close recv abort", test_bus1_close_recv_abort },
	{ "bus1 send no pipes", test_bus1_send_no_pipes },
	{ "bus1 cooked", test_bus1_cooked },
	{ NULL, NULL },
};
