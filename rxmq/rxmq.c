/*
 * rxmq - RabbitMQ (AMQP 0-9-1) client plugin for cREXX / RXPA
 *
 * Wraps librabbitmq-c.  Every REXX-visible connection is an integer handle
 * (1..MQ_MAX_HANDLES).  Each handle owns one TCP connection and one channel
 * (channel 1).  All procedures return an .int status:
 *
 *      0 (or a positive handle)  success
 *      1                         mqreceive: timeout, no message
 *     <0                         failure; mqerror(handle) gives the text
 *
 * Message bodies are passed as REXX strings, i.e. text.  A body containing
 * a NUL byte is truncated at the NUL when received.
 */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <sys/time.h>

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#include "crexxpa.h"

#define MQ_MAX_HANDLES 16
#define MQ_CHANNEL     1
#define MQ_ERRLEN      512

/* error codes */
#define MQE_BADHANDLE  (-1)
#define MQE_SOCKET     (-2)
#define MQE_OPEN       (-3)
#define MQE_LOGIN      (-4)
#define MQE_CHANNEL    (-5)
#define MQE_BROKER     (-6)
#define MQE_LIBRARY    (-7)
#define MQE_ARGUMENT   (-8)
#define MQE_NOSPACE    (-9)
#define MQE_STATE      (-10)

typedef struct {
    int used;
    int chan_dead;          /* broker closed the channel; reopen before further use */
    amqp_connection_state_t conn;
    char err[MQ_ERRLEN];
} mq_handle;

static mq_handle handles[MQ_MAX_HANDLES + 1];
static char global_err[MQ_ERRLEN];
static int mq_debug = 0;

#define DBG(...) do { if (mq_debug) { printf("MQ "); printf(__VA_ARGS__); printf("\n"); } } while (0)

/* ------------------------------------------------------------------ helpers */

static void set_err(mq_handle *h, const char *fmt, ...)
{
    va_list ap;
    char *dst = h ? h->err : global_err;
    va_start(ap, fmt);
    vsnprintf(dst, MQ_ERRLEN, fmt, ap);
    va_end(ap);
    if (h) {
        strncpy(global_err, dst, MQ_ERRLEN - 1);
        global_err[MQ_ERRLEN - 1] = '\0';
    }
    DBG("error: %s", dst);
}

static void clear_err(mq_handle *h)
{
    if (h) h->err[0] = '\0';
}

static mq_handle *get_handle(intptr_t id)
{
    if (id < 1 || id > MQ_MAX_HANDLES || !handles[id].used) {
        set_err(NULL, "invalid connection handle %ld", (long)id);
        return NULL;
    }
    return &handles[id];
}

static void note_channel_closed(mq_handle *h);

/* Turn an amqp_rpc_reply_t into 0 / negative code, recording text. */
static int check_reply(mq_handle *h, amqp_rpc_reply_t r, const char *ctx)
{
    switch (r.reply_type) {
    case AMQP_RESPONSE_NORMAL:
        return 0;
    case AMQP_RESPONSE_NONE:
        set_err(h, "%s: missing RPC reply type", ctx);
        return MQE_LIBRARY;
    case AMQP_RESPONSE_LIBRARY_EXCEPTION:
        set_err(h, "%s: %s", ctx, amqp_error_string2(r.library_error));
        return MQE_LIBRARY;
    case AMQP_RESPONSE_SERVER_EXCEPTION:
        switch (r.reply.id) {
        case AMQP_CONNECTION_CLOSE_METHOD: {
            amqp_connection_close_t *m = (amqp_connection_close_t *)r.reply.decoded;
            set_err(h, "%s: broker closed connection: %d %.*s", ctx, m->reply_code,
                    (int)m->reply_text.len, (char *)m->reply_text.bytes);
            return MQE_BROKER;
        }
        case AMQP_CHANNEL_CLOSE_METHOD: {
            amqp_channel_close_t *m = (amqp_channel_close_t *)r.reply.decoded;
            set_err(h, "%s: broker closed channel: %d %.*s", ctx, m->reply_code,
                    (int)m->reply_text.len, (char *)m->reply_text.bytes);
            note_channel_closed(h);
            return MQE_BROKER;
        }
        default:
            set_err(h, "%s: unknown broker exception 0x%X", ctx, (unsigned)r.reply.id);
            return MQE_BROKER;
        }
    }
    set_err(h, "%s: unrecognised reply", ctx);
    return MQE_LIBRARY;
}

/*
 * The broker closed our channel (for example a passive declare or purge of a
 * queue that does not exist).  AMQP requires the client to answer with
 * channel.close-ok; only then may the channel number be opened again.
 */
static void note_channel_closed(mq_handle *h)
{
    amqp_channel_close_ok_t ok;
    memset(&ok, 0, sizeof(ok));
    amqp_send_method(h->conn, MQ_CHANNEL, AMQP_CHANNEL_CLOSE_OK_METHOD, &ok);
    h->chan_dead = 1;
}

/* After a broker side channel close, open the channel again so the handle stays usable. */
static void reopen_channel_if_needed(mq_handle *h, int rc)
{
    (void)rc;
    if (h->chan_dead) {
        amqp_rpc_reply_t r;
        amqp_channel_open(h->conn, MQ_CHANNEL);
        r = amqp_get_rpc_reply(h->conn);
        if (r.reply_type == AMQP_RESPONSE_NORMAL) h->chan_dead = 0;
        else DBG("channel reopen failed");
    }
}

/* has the (case-insensitive) word in a space separated flag string? */
static int has_flag(const char *flags, const char *word)
{
    size_t wl = strlen(word);
    const char *p = flags;
    if (!flags) return 0;
    while (*p) {
        size_t n;
        while (*p && isspace((unsigned char)*p)) p++;
        n = 0;
        while (p[n] && !isspace((unsigned char)p[n])) n++;
        if (n == wl) {
            size_t i;
            int same = 1;
            for (i = 0; i < n; i++)
                if (tolower((unsigned char)p[i]) != tolower((unsigned char)word[i])) { same = 0; break; }
            if (same) return 1;
        }
        p += n;
    }
    return 0;
}

static char *dup_str(const char *s)
{
    size_t n = s ? strlen(s) : 0;
    char *d = (char *)malloc(n + 1);
    if (d) { if (n) memcpy(d, s, n); d[n] = '\0'; }
    return d;
}

static void close_handle(mq_handle *h)
{
    if (!h->used) return;
    if (!h->chan_dead) amqp_channel_close(h->conn, MQ_CHANNEL, AMQP_REPLY_SUCCESS);
    amqp_connection_close(h->conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(h->conn);
    memset(h, 0, sizeof(*h));
}

/* ------------------------------------------------------------ procedures */

/* mqconnect(broker, port, user, password, vhost) -> handle | <0 */
PROCEDURE(mqconnect) {
    const char *host  = GETSTRING(ARG0);
    int port          = (int)GETINT(ARG1);
    const char *user  = GETSTRING(ARG2);
    const char *pass  = GETSTRING(ARG3);
    const char *vhost = GETSTRING(ARG4);
    int id, rc;
    mq_handle *h = NULL;
    amqp_socket_t *sock;
    amqp_rpc_reply_t r;

    if (!host || !*host) host = "localhost";
    if (port <= 0) port = 5672;
    if (!user || !*user) user = "guest";
    if (!pass) pass = "guest";
    if (!vhost || !*vhost) vhost = "/";

    for (id = 1; id <= MQ_MAX_HANDLES; id++)
        if (!handles[id].used) { h = &handles[id]; break; }
    if (!h) { set_err(NULL, "no free connection handle (max %d)", MQ_MAX_HANDLES); RETURNINTX(MQE_NOSPACE); }

    memset(h, 0, sizeof(*h));
    h->conn = amqp_new_connection();
    sock = amqp_tcp_socket_new(h->conn);
    if (!sock) {
        set_err(NULL, "cannot create TCP socket");
        amqp_destroy_connection(h->conn);
        RETURNINTX(MQE_SOCKET);
    }
    rc = amqp_socket_open(sock, host, port);
    if (rc) {
        set_err(NULL, "cannot connect to %s:%d: %s", host, port, amqp_error_string2(rc));
        amqp_destroy_connection(h->conn);
        RETURNINTX(MQE_OPEN);
    }
    /* heartbeat 0: a REXX program may legitimately sit idle between calls */
    r = amqp_login(h->conn, vhost, 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, user, pass);
    h->used = 1;
    rc = check_reply(h, r, "login");
    if (rc) {
        amqp_destroy_connection(h->conn);
        memset(h, 0, sizeof(*h));
        RETURNINTX(MQE_LOGIN);
    }
    amqp_channel_open(h->conn, MQ_CHANNEL);
    r = amqp_get_rpc_reply(h->conn);
    rc = check_reply(h, r, "channel.open");
    if (rc) {
        amqp_connection_close(h->conn, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(h->conn);
        memset(h, 0, sizeof(*h));
        RETURNINTX(MQE_CHANNEL);
    }
    DBG("connected %s:%d vhost %s handle %d", host, port, vhost, id);
    RETURNINTX(id);
    ENDPROC
}

/* mqdisconnect(handle) -> 0 | <0 */
PROCEDURE(mqdisconnect) {
    mq_handle *h = get_handle(GETINT(ARG0));
    if (!h) RETURNINTX(MQE_BADHANDLE);
    close_handle(h);
    RETURNINTX(0);
    ENDPROC
}

/* mqexchange(handle, name, type, flags) -> 0 | <0   flags: DURABLE AUTODELETE */
PROCEDURE(mqexchange) {
    mq_handle *h = get_handle(GETINT(ARG0));
    const char *name  = GETSTRING(ARG1);
    const char *type  = GETSTRING(ARG2);
    const char *flags = GETSTRING(ARG3);
    int rc;
    if (!h) RETURNINTX(MQE_BADHANDLE);
    if (!name || !*name) { set_err(h, "exchange name required"); RETURNINTX(MQE_ARGUMENT); }
    if (!type || !*type) type = "direct";
    clear_err(h);
    amqp_exchange_declare(h->conn, MQ_CHANNEL, amqp_cstring_bytes(name), amqp_cstring_bytes(type),
                          0, has_flag(flags, "DURABLE"), has_flag(flags, "AUTODELETE"), 0,
                          amqp_empty_table);
    rc = check_reply(h, amqp_get_rpc_reply(h->conn), "exchange.declare");
    reopen_channel_if_needed(h, rc);
    RETURNINTX(rc);
    ENDPROC
}

/* mqqueue(handle, name, flags) -> 0 | <0   flags: DURABLE EXCLUSIVE AUTODELETE PASSIVE */
PROCEDURE(mqqueue) {
    mq_handle *h = get_handle(GETINT(ARG0));
    const char *name  = GETSTRING(ARG1);
    const char *flags = GETSTRING(ARG2);
    int rc;
    if (!h) RETURNINTX(MQE_BADHANDLE);
    if (!name || !*name) { set_err(h, "queue name required (use mqtempqueue for a server-named queue)"); RETURNINTX(MQE_ARGUMENT); }
    clear_err(h);
    amqp_queue_declare(h->conn, MQ_CHANNEL, amqp_cstring_bytes(name),
                       has_flag(flags, "PASSIVE"), has_flag(flags, "DURABLE"),
                       has_flag(flags, "EXCLUSIVE"), has_flag(flags, "AUTODELETE"),
                       amqp_empty_table);
    rc = check_reply(h, amqp_get_rpc_reply(h->conn), "queue.declare");
    reopen_channel_if_needed(h, rc);
    RETURNINTX(rc);
    ENDPROC
}

/* mqtempqueue(handle) -> broker generated name of an exclusive auto-delete queue, "" on error */
PROCEDURE(mqtempqueue) {
    mq_handle *h = get_handle(GETINT(ARG0));
    amqp_queue_declare_ok_t *ok;
    char *name;
    if (!h) RETURNSTRX("");
    clear_err(h);
    ok = amqp_queue_declare(h->conn, MQ_CHANNEL, amqp_empty_bytes, 0, 0, 1, 1, amqp_empty_table);
    if (check_reply(h, amqp_get_rpc_reply(h->conn), "queue.declare") || !ok) {
        if (!ok && !h->err[0]) set_err(h, "queue.declare returned no result");
        RETURNSTRX("");
    }
    name = (char *)malloc(ok->queue.len + 1);
    if (!name) { set_err(h, "out of memory"); RETURNSTRX(""); }
    memcpy(name, ok->queue.bytes, ok->queue.len);
    name[ok->queue.len] = '\0';
    RETURNSTR(name);
    free(name);
    ENDPROC
}

/* mqpurge(handle, queue) -> number of messages purged | <0 */
PROCEDURE(mqpurge) {
    mq_handle *h = get_handle(GETINT(ARG0));
    const char *queue = GETSTRING(ARG1);
    amqp_queue_purge_ok_t *ok;
    int rc;
    if (!h) RETURNINTX(MQE_BADHANDLE);
    clear_err(h);
    ok = amqp_queue_purge(h->conn, MQ_CHANNEL, amqp_cstring_bytes(queue));
    rc = check_reply(h, amqp_get_rpc_reply(h->conn), "queue.purge");
    reopen_channel_if_needed(h, rc);
    if (rc) RETURNINTX(rc);
    RETURNINTX(ok ? (intptr_t)ok->message_count : 0);
    ENDPROC
}

/* mqdelete(handle, queue) -> 0 | <0 */
PROCEDURE(mqdelete) {
    mq_handle *h = get_handle(GETINT(ARG0));
    const char *queue = GETSTRING(ARG1);
    int rc;
    if (!h) RETURNINTX(MQE_BADHANDLE);
    clear_err(h);
    amqp_queue_delete(h->conn, MQ_CHANNEL, amqp_cstring_bytes(queue), 0, 0);
    rc = check_reply(h, amqp_get_rpc_reply(h->conn), "queue.delete");
    reopen_channel_if_needed(h, rc);
    RETURNINTX(rc);
    ENDPROC
}

/* mqbind(handle, queue, exchange, routingkey) -> 0 | <0 */
PROCEDURE(mqbind) {
    mq_handle *h = get_handle(GETINT(ARG0));
    const char *queue = GETSTRING(ARG1);
    const char *exch  = GETSTRING(ARG2);
    const char *key   = GETSTRING(ARG3);
    int rc;
    if (!h) RETURNINTX(MQE_BADHANDLE);
    if (!key) key = "";
    clear_err(h);
    amqp_queue_bind(h->conn, MQ_CHANNEL, amqp_cstring_bytes(queue), amqp_cstring_bytes(exch),
                    amqp_cstring_bytes(key), amqp_empty_table);
    rc = check_reply(h, amqp_get_rpc_reply(h->conn), "queue.bind");
    reopen_channel_if_needed(h, rc);
    RETURNINTX(rc);
    ENDPROC
}

/*
 * mqpublish(handle, exchange, routingkey, body, props) -> 0 | <0
 *
 * exchange "" publishes through the default exchange, i.e. routingkey is a
 * queue name.  props is a space separated list of
 *      reply_to=<queue>  correlation_id=<id>  content_type=<type>
 *      expiration=<ms>   persistent
 * (values cannot contain blanks).
 */
PROCEDURE(mqpublish) {
    mq_handle *h = get_handle(GETINT(ARG0));
    const char *exch  = GETSTRING(ARG1);
    const char *key   = GETSTRING(ARG2);
    const char *body  = GETSTRING(ARG3);
    const char *props = GETSTRING(ARG4);
    amqp_basic_properties_t bp;
    char *pcopy, *tok, *save = NULL;
    int rc;

    if (!h) RETURNINTX(MQE_BADHANDLE);
    if (!exch) exch = "";
    if (!key) key = "";
    if (!body) body = "";
    clear_err(h);

    memset(&bp, 0, sizeof(bp));
    bp._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
    bp.content_type = amqp_cstring_bytes("text/plain");
    bp.delivery_mode = 1;

    pcopy = dup_str(props ? props : "");
    if (!pcopy) { set_err(h, "out of memory"); RETURNINTX(MQE_LIBRARY); }
    for (tok = strtok_r(pcopy, " \t\r\n", &save); tok; tok = strtok_r(NULL, " \t\r\n", &save)) {
        char *eq = strchr(tok, '=');
        if (eq) {
            *eq++ = '\0';
            if (!strcasecmp(tok, "reply_to")) {
                bp._flags |= AMQP_BASIC_REPLY_TO_FLAG; bp.reply_to = amqp_cstring_bytes(eq);
            } else if (!strcasecmp(tok, "correlation_id")) {
                bp._flags |= AMQP_BASIC_CORRELATION_ID_FLAG; bp.correlation_id = amqp_cstring_bytes(eq);
            } else if (!strcasecmp(tok, "content_type")) {
                bp.content_type = amqp_cstring_bytes(eq);
            } else if (!strcasecmp(tok, "expiration")) {
                bp._flags |= AMQP_BASIC_EXPIRATION_FLAG; bp.expiration = amqp_cstring_bytes(eq);
            } else {
                set_err(h, "unknown property '%s'", tok);
                free(pcopy);
                RETURNINTX(MQE_ARGUMENT);
            }
        } else if (!strcasecmp(tok, "persistent")) {
            bp.delivery_mode = 2;
        } else {
            set_err(h, "unknown property '%s'", tok);
            free(pcopy);
            RETURNINTX(MQE_ARGUMENT);
        }
    }

    rc = amqp_basic_publish(h->conn, MQ_CHANNEL, amqp_cstring_bytes(exch), amqp_cstring_bytes(key),
                            0, 0, &bp, amqp_cstring_bytes(body));
    free(pcopy);
    if (rc != AMQP_STATUS_OK) {
        set_err(h, "basic.publish: %s", amqp_error_string2(rc));
        RETURNINTX(MQE_LIBRARY);
    }
    RETURNINTX(0);
    ENDPROC
}

/* mqsubscribe(handle, queue, prefetch) -> 0 | <0   (manual acknowledgement) */
PROCEDURE(mqsubscribe) {
    mq_handle *h = get_handle(GETINT(ARG0));
    const char *queue = GETSTRING(ARG1);
    int prefetch = (int)GETINT(ARG2);
    int rc;
    if (!h) RETURNINTX(MQE_BADHANDLE);
    clear_err(h);
    if (prefetch < 0) prefetch = 0;
    amqp_basic_qos(h->conn, MQ_CHANNEL, 0, (uint16_t)prefetch, 0);
    rc = check_reply(h, amqp_get_rpc_reply(h->conn), "basic.qos");
    if (rc) RETURNINTX(rc);
    amqp_basic_consume(h->conn, MQ_CHANNEL, amqp_cstring_bytes(queue), amqp_empty_bytes,
                       0, 0, 0, amqp_empty_table);
    rc = check_reply(h, amqp_get_rpc_reply(h->conn), "basic.consume");
    reopen_channel_if_needed(h, rc);
    RETURNINTX(rc);
    ENDPROC
}

/*
 * mqreceive(handle, timeout_ms, expose msg=.string[]) -> 0 | 1 | <0
 *   0  message received, msg filled in:
 *        msg[1] body            msg[2] delivery tag    msg[3] reply_to
 *        msg[4] correlation_id  msg[5] exchange        msg[6] routing key
 *        msg[7] redelivered (0/1)
 *   1  timeout (msg untouched)
 * timeout_ms < 0 waits forever.
 */
PROCEDURE(mqreceive) {
    mq_handle *h = get_handle(GETINT(ARG0));
    intptr_t timeout_ms = (intptr_t)GETINT(ARG1);
    amqp_rpc_reply_t r;
    amqp_envelope_t env;
    struct timeval tv, *ptv = NULL;
    char tag[32], redel[2];
    char *body;
    amqp_basic_properties_t *p;

    if (!h) RETURNINTX(MQE_BADHANDLE);
    clear_err(h);
    if (timeout_ms >= 0) {
        tv.tv_sec = (time_t)(timeout_ms / 1000);
        tv.tv_usec = (suseconds_t)((timeout_ms % 1000) * 1000);
        ptv = &tv;
    }
    amqp_maybe_release_buffers(h->conn);
    r = amqp_consume_message(h->conn, &env, ptv, 0);
    if (r.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION && r.library_error == AMQP_STATUS_TIMEOUT)
        RETURNINTX(1);
    if (r.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION && r.library_error == AMQP_STATUS_UNEXPECTED_STATE) {
        /* a non-delivery frame arrived: most likely the broker closing the channel */
        amqp_frame_t frame;
        struct timeval none = { 0, 0 };
        if (amqp_simple_wait_frame_noblock(h->conn, &frame, &none) == AMQP_STATUS_OK &&
            frame.frame_type == AMQP_FRAME_METHOD &&
            frame.payload.method.id == AMQP_CHANNEL_CLOSE_METHOD) {
            amqp_channel_close_t *m = (amqp_channel_close_t *)frame.payload.method.decoded;
            set_err(h, "consume: broker closed channel: %d %.*s (subscriptions are lost)", m->reply_code,
                    (int)m->reply_text.len, (char *)m->reply_text.bytes);
            note_channel_closed(h);
            reopen_channel_if_needed(h, MQE_BROKER);
            RETURNINTX(MQE_BROKER);
        }
        set_err(h, "consume: unexpected protocol state");
        RETURNINTX(MQE_STATE);
    }
    if (r.reply_type != AMQP_RESPONSE_NORMAL) {
        RETURNINTX(check_reply(h, r, "consume"));
    }

    p = &env.message.properties;
    body = (char *)malloc(env.message.body.len + 1);
    if (!body) {
        set_err(h, "out of memory");
        amqp_destroy_envelope(&env);
        RETURNINTX(MQE_LIBRARY);
    }
    if (env.message.body.len) memcpy(body, env.message.body.bytes, env.message.body.len);
    body[env.message.body.len] = '\0';

    snprintf(tag, sizeof(tag), "%llu", (unsigned long long)env.delivery_tag);
    redel[0] = env.redelivered ? '1' : '0';
    redel[1] = '\0';

    SETARRAYHI(ARG2, 7);
    SETSARRAY(ARG2, 0, body);
    SETSARRAY(ARG2, 1, tag);

#define BYTES_TO_ARRAY(idx, flag, field) do {                                          \
        if ((p->_flags & (flag)) && p->field.len) {                                     \
            char *s = (char *)malloc(p->field.len + 1);                                 \
            if (s) { memcpy(s, p->field.bytes, p->field.len); s[p->field.len] = '\0';   \
                     SETSARRAY(ARG2, (idx), s); free(s); }                              \
        } else SETSARRAY(ARG2, (idx), "");                                              \
    } while (0)
    BYTES_TO_ARRAY(2, AMQP_BASIC_REPLY_TO_FLAG, reply_to);
    BYTES_TO_ARRAY(3, AMQP_BASIC_CORRELATION_ID_FLAG, correlation_id);
#undef BYTES_TO_ARRAY
    {
        char *s = (char *)malloc(env.exchange.len + 1);
        if (s) { memcpy(s, env.exchange.bytes, env.exchange.len); s[env.exchange.len] = '\0';
                 SETSARRAY(ARG2, 4, s); free(s); }
        s = (char *)malloc(env.routing_key.len + 1);
        if (s) { memcpy(s, env.routing_key.bytes, env.routing_key.len); s[env.routing_key.len] = '\0';
                 SETSARRAY(ARG2, 5, s); free(s); }
    }
    SETSARRAY(ARG2, 6, redel);

    free(body);
    amqp_destroy_envelope(&env);
    RETURNINTX(0);
    ENDPROC
}

/* mqack(handle, tag) -> 0 | <0 */
PROCEDURE(mqack) {
    mq_handle *h = get_handle(GETINT(ARG0));
    const char *tag = GETSTRING(ARG1);
    int rc;
    if (!h) RETURNINTX(MQE_BADHANDLE);
    clear_err(h);
    rc = amqp_basic_ack(h->conn, MQ_CHANNEL, (uint64_t)strtoull(tag ? tag : "0", NULL, 10), 0);
    if (rc != AMQP_STATUS_OK) { set_err(h, "basic.ack: %s", amqp_error_string2(rc)); RETURNINTX(MQE_LIBRARY); }
    RETURNINTX(0);
    ENDPROC
}

/* mqnack(handle, tag, requeue) -> 0 | <0   requeue: 1 = put back on the queue, 0 = discard/dead-letter */
PROCEDURE(mqnack) {
    mq_handle *h = get_handle(GETINT(ARG0));
    const char *tag = GETSTRING(ARG1);
    int requeue = (int)GETINT(ARG2);
    int rc;
    if (!h) RETURNINTX(MQE_BADHANDLE);
    clear_err(h);
    rc = amqp_basic_nack(h->conn, MQ_CHANNEL, (uint64_t)strtoull(tag ? tag : "0", NULL, 10), 0, requeue != 0);
    if (rc != AMQP_STATUS_OK) { set_err(h, "basic.nack: %s", amqp_error_string2(rc)); RETURNINTX(MQE_LIBRARY); }
    RETURNINTX(0);
    ENDPROC
}

/* mqerror(handle) -> text of the last error; handle 0 = last error of any connection attempt */
PROCEDURE(mqerror) {
    intptr_t id = (intptr_t)GETINT(ARG0);
    if (id >= 1 && id <= MQ_MAX_HANDLES && handles[id].used) RETURNSTRX(handles[id].err);
    RETURNSTRX(global_err);
    ENDPROC
}

/* mqflags(flags) -> 0     flags: DEBUG NDEBUG */
PROCEDURE(mqflags) {
    const char *flags = GETSTRING(ARG0);
    if (has_flag(flags, "DEBUG")) mq_debug = 1;
    if (has_flag(flags, "NDEBUG")) mq_debug = 0;
    RETURNINTX(0);
    ENDPROC
}

LOADFUNCS
    ADDPROC(mqflags,      "rxmq.mqflags",      "b", ".int",    "flags=.string");
    ADDPROC(mqconnect,    "rxmq.mqconnect",    "b", ".int",    "broker=.string,port=.int,user=.string,password=.string,vhost=.string");
    ADDPROC(mqdisconnect, "rxmq.mqdisconnect", "b", ".int",    "handle=.int");
    ADDPROC(mqexchange,   "rxmq.mqexchange",   "b", ".int",    "handle=.int,name=.string,type=.string,flags=.string");
    ADDPROC(mqqueue,      "rxmq.mqqueue",      "b", ".int",    "handle=.int,name=.string,flags=.string");
    ADDPROC(mqtempqueue,  "rxmq.mqtempqueue",  "b", ".string", "handle=.int");
    ADDPROC(mqpurge,      "rxmq.mqpurge",      "b", ".int",    "handle=.int,queue=.string");
    ADDPROC(mqdelete,     "rxmq.mqdelete",     "b", ".int",    "handle=.int,queue=.string");
    ADDPROC(mqbind,       "rxmq.mqbind",       "b", ".int",    "handle=.int,queue=.string,exchange=.string,routingkey=.string");
    ADDPROC(mqpublish,    "rxmq.mqpublish",    "b", ".int",    "handle=.int,exchange=.string,routingkey=.string,body=.string,props=.string");
    ADDPROC(mqsubscribe,  "rxmq.mqsubscribe",  "b", ".int",    "handle=.int,queue=.string,prefetch=.int");
    ADDPROC(mqreceive,    "rxmq.mqreceive",    "b", ".int",    "handle=.int,timeout=.int,expose msg=.string[]");
    ADDPROC(mqack,        "rxmq.mqack",        "b", ".int",    "handle=.int,tag=.string");
    ADDPROC(mqnack,       "rxmq.mqnack",       "b", ".int",    "handle=.int,tag=.string,requeue=.int");
    ADDPROC(mqerror,      "rxmq.mqerror",      "b", ".string", "handle=.int");
ENDLOADFUNCS
