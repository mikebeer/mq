/*
 * mqcore - AMQP 0-9-1 client core on librabbitmq-c, plain C.  See mqcore.h.
 *
 * Split out of rxmq.c so that programs other than the cREXX plugin can use the same logic.
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

#include "mqcore.h"

#define MQC_CHANNEL 1
#define MQC_ERRLEN  512

/* error codes */

/* a delivery taken off the wire while waiting for a publisher confirm, kept for mqreceive */
typedef struct pending {
    struct pending *next;
    char *body;
    size_t body_len;
    unsigned long long tag;
    char *reply_to, *corr, *exchange, *key;
    int redelivered;
} pending;

typedef struct {
    int used;
    int chan_dead;          /* broker closed the channel; reopen before further use */
    int conn_dead;          /* the connection is gone (closed by the broker or socket error): every call fails with -6 */
    int confirm;            /* publisher confirms on: mqpublish waits for the broker's answer */
    uint64_t pub_seq;       /* number of publishes since confirm.select on this channel */
    pending *phead, *ptail;
    amqp_connection_state_t conn;
    char err[MQC_ERRLEN];
} mq_handle;

static mq_handle handles[MQC_MAX_HANDLES + 1];
static char global_err[MQC_ERRLEN];
static int mqc_debug = 0;

#define DBG(...) do { if (mqc_debug) { printf("MQ "); printf(__VA_ARGS__); printf("\n"); } } while (0)

/* ------------------------------------------------------------------ helpers */

static void set_err(mq_handle *h, const char *fmt, ...)
{
    va_list ap;
    char *dst = h ? h->err : global_err;
    va_start(ap, fmt);
    vsnprintf(dst, MQC_ERRLEN, fmt, ap);
    va_end(ap);
    if (h) {
        strncpy(global_err, dst, MQC_ERRLEN - 1);
        global_err[MQC_ERRLEN - 1] = '\0';
    }
    DBG("error: %s", dst);
}

static void clear_err(mq_handle *h)
{
    if (h) h->err[0] = '\0';
}

static mq_handle *get_handle(intptr_t id)
{
    if (id < 1 || id > MQC_MAX_HANDLES || !handles[id].used) {
        set_err(NULL, "invalid connection handle %ld", (long)id);
        return NULL;
    }
    return &handles[id];
}

static void note_channel_closed(mq_handle *h);

/* The broker closed the connection, or the socket failed: nothing more can be done with this handle
   except mqdisconnect.  Answer connection.close politely if the socket still allows it. */
static void note_connection_dead(mq_handle *h, int answer_close)
{
    if (answer_close && !h->conn_dead) {
        amqp_connection_close_ok_t ok;
        memset(&ok, 0, sizeof(ok));
        amqp_send_method(h->conn, 0, AMQP_CONNECTION_CLOSE_OK_METHOD, &ok);
    }
    h->conn_dead = 1;
}

static int is_fatal_library_error(int e)
{
    return e == AMQP_STATUS_CONNECTION_CLOSED || e == AMQP_STATUS_SOCKET_ERROR ||
           e == AMQP_STATUS_TCP_ERROR || e == AMQP_STATUS_SSL_ERROR || e == AMQP_STATUS_HEARTBEAT_TIMEOUT ||
           e == AMQP_STATUS_BAD_AMQP_DATA;
}
static void free_pending_list(mq_handle *h);

/* Turn an amqp_rpc_reply_t into 0 / negative code, recording text. */
static int check_reply(mq_handle *h, amqp_rpc_reply_t r, const char *ctx)
{
    switch (r.reply_type) {
    case AMQP_RESPONSE_NORMAL:
        return 0;
    case AMQP_RESPONSE_NONE:
        set_err(h, "%s: missing RPC reply type", ctx);
        return MQC_E_LIBRARY;
    case AMQP_RESPONSE_LIBRARY_EXCEPTION:
        set_err(h, "%s: %s", ctx, amqp_error_string2(r.library_error));
        if (h && is_fatal_library_error(r.library_error)) { note_connection_dead(h, 0); return MQC_E_BROKER; }
        return MQC_E_LIBRARY;
    case AMQP_RESPONSE_SERVER_EXCEPTION:
        switch (r.reply.id) {
        case AMQP_CONNECTION_CLOSE_METHOD: {
            amqp_connection_close_t *m = (amqp_connection_close_t *)r.reply.decoded;
            set_err(h, "%s: broker closed connection: %d %.*s", ctx, m->reply_code,
                    (int)m->reply_text.len, (char *)m->reply_text.bytes);
            if (h) note_connection_dead(h, 1);
            return MQC_E_BROKER;
        }
        case AMQP_CHANNEL_CLOSE_METHOD: {
            amqp_channel_close_t *m = (amqp_channel_close_t *)r.reply.decoded;
            set_err(h, "%s: broker closed channel: %d %.*s", ctx, m->reply_code,
                    (int)m->reply_text.len, (char *)m->reply_text.bytes);
            note_channel_closed(h);
            return MQC_E_BROKER;
        }
        default:
            set_err(h, "%s: unknown broker exception 0x%X", ctx, (unsigned)r.reply.id);
            return MQC_E_BROKER;
        }
    }
    set_err(h, "%s: unrecognised reply", ctx);
    return MQC_E_LIBRARY;
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
    amqp_send_method(h->conn, MQC_CHANNEL, AMQP_CHANNEL_CLOSE_OK_METHOD, &ok);
    h->chan_dead = 1;
    /* deliveries set aside earlier belong to the dead channel: their tags are void and the broker
       puts the unacknowledged messages back on their queues, so drop our copies */
    free_pending_list(h);
}

/* After a broker side channel close, open the channel again so the handle stays usable. */
static void reopen_channel_if_needed(mq_handle *h, int rc)
{
    (void)rc;
    if (h->chan_dead) {
        amqp_rpc_reply_t r;
        amqp_channel_open(h->conn, MQC_CHANNEL);
        r = amqp_get_rpc_reply(h->conn);
        if (r.reply_type == AMQP_RESPONSE_NORMAL) {
            h->chan_dead = 0;
            h->pub_seq = 0;
            if (h->confirm) {           /* confirm mode belongs to the channel: ask for it again */
                amqp_confirm_select(h->conn, MQC_CHANNEL);
                r = amqp_get_rpc_reply(h->conn);
                if (r.reply_type != AMQP_RESPONSE_NORMAL) h->confirm = 0;
            }
        }
        else DBG("channel reopen failed");
    }
}

static char *dup_str(const char *s)
{
    size_t n = s ? strlen(s) : 0;
    char *d = (char *)malloc(n + 1);
    if (d) { if (n) memcpy(d, s, n); d[n] = '\0'; }
    return d;
}

static char *copy_bytes(const void *b, size_t n)
{
    char *d = (char *)malloc(n + 1);
    if (d) { if (n) memcpy(d, b, n); d[n] = '\0'; }
    return d;
}

static void free_pending(pending *n)
{
    free(n->body); free(n->reply_to); free(n->corr); free(n->exchange); free(n->key); free(n);
}

/* Keep a delivery (method fields plus message) for mqc_receive.  Returns 0 or MQC_E_LIBRARY. */
static int queue_pending(mq_handle *h, uint64_t tag, int redelivered, amqp_bytes_t exch, amqp_bytes_t key,
                         const amqp_basic_properties_t *p, amqp_bytes_t body)
{
    pending *n = (pending *)calloc(1, sizeof(*n));
    if (!n) return MQC_E_LIBRARY;
    n->body = copy_bytes(body.bytes, body.len);
    n->body_len = body.len;
    n->tag = (unsigned long long)tag;
    n->reply_to = (p->_flags & AMQP_BASIC_REPLY_TO_FLAG) ? copy_bytes(p->reply_to.bytes, p->reply_to.len) : dup_str("");
    n->corr = (p->_flags & AMQP_BASIC_CORRELATION_ID_FLAG) ? copy_bytes(p->correlation_id.bytes, p->correlation_id.len) : dup_str("");
    n->exchange = copy_bytes(exch.bytes, exch.len);
    n->key = copy_bytes(key.bytes, key.len);
    n->redelivered = redelivered ? 1 : 0;
    if (!n->body || !n->reply_to || !n->corr || !n->exchange || !n->key) { free_pending(n); return MQC_E_LIBRARY; }
    if (h->ptail) h->ptail->next = n; else h->phead = n;
    h->ptail = n;
    return 0;
}

static void free_pending_list(mq_handle *h)
{
    while (h->phead) { pending *n = h->phead->next; free_pending(h->phead); h->phead = n; }
    h->ptail = NULL;
}

static void close_handle(mq_handle *h)
{
    if (!h->used) return;
    free_pending_list(h);
    if (!h->conn_dead) {
        if (!h->chan_dead) amqp_channel_close(h->conn, MQC_CHANNEL, AMQP_REPLY_SUCCESS);
        amqp_connection_close(h->conn, AMQP_REPLY_SUCCESS);
    }
    amqp_destroy_connection(h->conn);
    memset(h, 0, sizeof(*h));
}
/*
 * Publisher confirms: wait for the broker to accept (basic.ack) or refuse (basic.nack) the message
 * just published, and for a basic.return when no queue could take it (we publish mandatory).
 * Deliveries that arrive meanwhile are set aside for mqreceive.
 */
static int wait_confirm(mq_handle *h, const char *exch, const char *key)
{
    uint64_t want = ++h->pub_seq;
    int returned = 0, rcode = 0;
    char rtext[128] = "";
    struct timeval tv;
    long waited = 0;

    for (;;) {
        amqp_frame_t f;
        int rc;
        tv.tv_sec = 1; tv.tv_usec = 0;
        rc = amqp_simple_wait_frame_noblock(h->conn, &f, &tv);
        if (rc == AMQP_STATUS_TIMEOUT) {
            if (++waited >= 10) { set_err(h, "publish: the broker did not confirm within 10 seconds"); return MQC_E_BROKER; }
            continue;
        }
        if (rc != AMQP_STATUS_OK) {
            set_err(h, "publish: %s", amqp_error_string2(rc));
            if (is_fatal_library_error(rc)) { note_connection_dead(h, 0); return MQC_E_BROKER; }
            return MQC_E_LIBRARY;
        }
        if (f.frame_type != AMQP_FRAME_METHOD) continue;
        switch (f.payload.method.id) {
        case AMQP_BASIC_RETURN_METHOD: {
            amqp_basic_return_t *m = (amqp_basic_return_t *)f.payload.method.decoded;
            amqp_message_t msg;
            returned = 1; rcode = m->reply_code;
            snprintf(rtext, sizeof(rtext), "%.*s", (int)m->reply_text.len, (char *)m->reply_text.bytes);
            if (amqp_read_message(h->conn, f.channel, &msg, 0).reply_type == AMQP_RESPONSE_NORMAL)
                amqp_destroy_message(&msg);
            break;
        }
        case AMQP_BASIC_ACK_METHOD: {
            amqp_basic_ack_t *m = (amqp_basic_ack_t *)f.payload.method.decoded;
            if (m->delivery_tag == want || (m->multiple && m->delivery_tag >= want)) {
                if (returned) {
                    set_err(h, "publish: not delivered to any queue (%d %s) - exchange '%s', routing key '%s'",
                            rcode, rtext, exch, key);
                    return MQC_E_UNROUTABLE;
                }
                return 0;
            }
            break;
        }
        case AMQP_BASIC_NACK_METHOD: {
            amqp_basic_nack_t *m = (amqp_basic_nack_t *)f.payload.method.decoded;
            if (m->delivery_tag == want || (m->multiple && m->delivery_tag >= want)) {
                set_err(h, "publish: the broker refused the message (basic.nack)");
                return MQC_E_BROKER;
            }
            break;
        }
        case AMQP_BASIC_DELIVER_METHOD: {
            amqp_basic_deliver_t *m = (amqp_basic_deliver_t *)f.payload.method.decoded;
            amqp_message_t msg;
            if (amqp_read_message(h->conn, f.channel, &msg, 0).reply_type == AMQP_RESPONSE_NORMAL) {
                queue_pending(h, m->delivery_tag, m->redelivered, m->exchange, m->routing_key,
                              &msg.properties, msg.body);
                amqp_destroy_message(&msg);
            }
            break;
        }
        case AMQP_CHANNEL_CLOSE_METHOD: {
            amqp_channel_close_t *m = (amqp_channel_close_t *)f.payload.method.decoded;
            set_err(h, "publish: broker closed channel: %d %.*s (the message was not delivered)", m->reply_code,
                    (int)m->reply_text.len, (char *)m->reply_text.bytes);
            note_channel_closed(h);
            reopen_channel_if_needed(h, MQC_E_BROKER);
            return MQC_E_BROKER;
        }
        case AMQP_CONNECTION_CLOSE_METHOD: {
            amqp_connection_close_t *m = (amqp_connection_close_t *)f.payload.method.decoded;
            set_err(h, "publish: broker closed connection: %d %.*s", m->reply_code,
                    (int)m->reply_text.len, (char *)m->reply_text.bytes);
            note_connection_dead(h, 1);
            return MQC_E_BROKER;
        }
        default:
            break;
        }
    }
}

/* ------------------------------------------------------------ public API */

#define NEED_HANDLE(h, id) \
    do { \
        (h) = get_handle(id); \
        if (!(h)) return MQC_E_BADHANDLE; \
        if ((h)->conn_dead) { set_err((h), "connection lost: the broker closed the connection (use mqc_disconnect and connect again)"); return MQC_E_BROKER; } \
    } while (0)

void mqc_set_debug(int on) { mqc_debug = on ? 1 : 0; }

const char *mqc_error(int id)
{
    if (id >= 1 && id <= MQC_MAX_HANDLES && handles[id].used) return handles[id].err;
    return global_err;
}

void mqc_set_error(int id, const char *text)
{
    mq_handle *h = (id >= 1 && id <= MQC_MAX_HANDLES && handles[id].used) ? &handles[id] : NULL;
    set_err(h, "%s", text ? text : "");
}

int mqc_connect(const char *host, int port, const char *user, const char *pass, const char *vhost)
{
    int id, rc;
    mq_handle *h = NULL;
    amqp_socket_t *sock;
    amqp_rpc_reply_t r;

    if (!host || !*host) host = "localhost";
    if (port <= 0) port = 5672;
    if (!user || !*user) user = "guest";
    if (!pass) pass = "guest";
    if (!vhost || !*vhost) vhost = "/";

    for (id = 1; id <= MQC_MAX_HANDLES; id++)
        if (!handles[id].used) { h = &handles[id]; break; }
    if (!h) { set_err(NULL, "no free connection handle (max %d)", MQC_MAX_HANDLES); return MQC_E_NOSPACE; }

    memset(h, 0, sizeof(*h));
    h->conn = amqp_new_connection();
    sock = amqp_tcp_socket_new(h->conn);
    if (!sock) {
        set_err(NULL, "cannot create TCP socket");
        amqp_destroy_connection(h->conn);
        return MQC_E_SOCKET;
    }
    rc = amqp_socket_open(sock, host, port);
    if (rc) {
        set_err(NULL, "tcp connect: %s", amqp_error_string2(rc));
        amqp_destroy_connection(h->conn);
        return MQC_E_OPEN;
    }
    /* heartbeat 0: a program may legitimately sit idle between calls */
    r = amqp_login(h->conn, vhost, 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, user, pass);
    h->used = 1;
    rc = check_reply(h, r, "login");
    if (rc) {
        amqp_destroy_connection(h->conn);
        memset(h, 0, sizeof(*h));
        return MQC_E_LOGIN;
    }
    amqp_channel_open(h->conn, MQC_CHANNEL);
    r = amqp_get_rpc_reply(h->conn);
    rc = check_reply(h, r, "channel.open");
    if (rc) {
        amqp_connection_close(h->conn, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(h->conn);
        memset(h, 0, sizeof(*h));
        return MQC_E_CHANNEL;
    }
    DBG("connected %s:%d vhost %s handle %d", host, port, vhost, id);
    return id;
}

int mqc_disconnect(int id)
{
    mq_handle *h = get_handle(id);
    if (!h) return MQC_E_BADHANDLE;
    close_handle(h);
    return 0;
}

int mqc_exchange(int id, const char *name, const char *type, int flags)
{
    mq_handle *h;
    int rc;
    NEED_HANDLE(h, id);
    if (!name || !*name) { set_err(h, "exchange name required"); return MQC_E_ARGUMENT; }
    if (!type || !*type) type = "direct";
    clear_err(h);
    amqp_exchange_declare(h->conn, MQC_CHANNEL, amqp_cstring_bytes(name), amqp_cstring_bytes(type),
                          0, (flags & MQC_DURABLE) != 0, (flags & MQC_AUTODELETE) != 0, 0, amqp_empty_table);
    rc = check_reply(h, amqp_get_rpc_reply(h->conn), "exchange.declare");
    reopen_channel_if_needed(h, rc);
    return rc;
}

int mqc_queue(int id, const char *name, int flags)
{
    mq_handle *h;
    int rc;
    NEED_HANDLE(h, id);
    if (!name || !*name) { set_err(h, "queue name required (use mqtempqueue for a server-named queue)"); return MQC_E_ARGUMENT; }
    clear_err(h);
    amqp_queue_declare(h->conn, MQC_CHANNEL, amqp_cstring_bytes(name),
                       (flags & MQC_PASSIVE) != 0, (flags & MQC_DURABLE) != 0,
                       (flags & MQC_EXCLUSIVE) != 0, (flags & MQC_AUTODELETE) != 0, amqp_empty_table);
    rc = check_reply(h, amqp_get_rpc_reply(h->conn), "queue.declare");
    reopen_channel_if_needed(h, rc);
    return rc;
}

int mqc_tempqueue(int id, char *name, size_t namelen)
{
    mq_handle *h;
    amqp_queue_declare_ok_t *ok;
    NEED_HANDLE(h, id);
    if (!name || namelen < 2) { set_err(h, "name buffer too small"); return MQC_E_ARGUMENT; }
    name[0] = '\0';
    clear_err(h);
    ok = amqp_queue_declare(h->conn, MQC_CHANNEL, amqp_empty_bytes, 0, 0, 1, 1, amqp_empty_table);
    {
        int rc = check_reply(h, amqp_get_rpc_reply(h->conn), "queue.declare");
        if (rc) return rc;
    }
    if (!ok) { set_err(h, "queue.declare returned no result"); return MQC_E_LIBRARY; }
    if (ok->queue.len + 1 > namelen) { set_err(h, "name buffer too small"); return MQC_E_ARGUMENT; }
    memcpy(name, ok->queue.bytes, ok->queue.len);
    name[ok->queue.len] = '\0';
    return 0;
}

int mqc_purge(int id, const char *queue)
{
    mq_handle *h;
    amqp_queue_purge_ok_t *ok;
    int rc;
    NEED_HANDLE(h, id);
    clear_err(h);
    ok = amqp_queue_purge(h->conn, MQC_CHANNEL, amqp_cstring_bytes(queue ? queue : ""));
    rc = check_reply(h, amqp_get_rpc_reply(h->conn), "queue.purge");
    reopen_channel_if_needed(h, rc);
    if (rc) return rc;
    return ok ? (int)ok->message_count : 0;
}

int mqc_delete(int id, const char *queue)
{
    mq_handle *h;
    int rc;
    NEED_HANDLE(h, id);
    clear_err(h);
    amqp_queue_delete(h->conn, MQC_CHANNEL, amqp_cstring_bytes(queue ? queue : ""), 0, 0);
    rc = check_reply(h, amqp_get_rpc_reply(h->conn), "queue.delete");
    reopen_channel_if_needed(h, rc);
    return rc;
}

int mqc_bind(int id, const char *queue, const char *exch, const char *key)
{
    mq_handle *h;
    int rc;
    NEED_HANDLE(h, id);
    if (!key) key = "";
    clear_err(h);
    amqp_queue_bind(h->conn, MQC_CHANNEL, amqp_cstring_bytes(queue ? queue : ""),
                    amqp_cstring_bytes(exch ? exch : ""), amqp_cstring_bytes(key), amqp_empty_table);
    rc = check_reply(h, amqp_get_rpc_reply(h->conn), "queue.bind");
    reopen_channel_if_needed(h, rc);
    return rc;
}


int mqc_confirm(int id)
{
    mq_handle *h;
    int rc = 0;
    NEED_HANDLE(h, id);
    clear_err(h);
    if (!h->confirm) {
        if (h->chan_dead) reopen_channel_if_needed(h, 0);
        amqp_confirm_select(h->conn, MQC_CHANNEL);
        rc = check_reply(h, amqp_get_rpc_reply(h->conn), "confirm.select");
        reopen_channel_if_needed(h, rc);
        if (rc == 0) { h->confirm = 1; h->pub_seq = 0; }
    }
    return rc;
}

int mqc_publish(int id, const char *exch, const char *key, const void *body, size_t body_len, const mqc_props *p)
{
    mq_handle *h;
    amqp_basic_properties_t bp;
    amqp_bytes_t payload;
    int rc;

    NEED_HANDLE(h, id);
    if (!exch) exch = "";
    if (!key) key = "";
    clear_err(h);

    memset(&bp, 0, sizeof(bp));
    bp._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
    bp.content_type = amqp_cstring_bytes("text/plain");
    bp.delivery_mode = 1;
    if (p) {
        if (p->persistent) bp.delivery_mode = 2;
        if (p->reply_to && *p->reply_to) { bp._flags |= AMQP_BASIC_REPLY_TO_FLAG; bp.reply_to = amqp_cstring_bytes(p->reply_to); }
        if (p->correlation_id && *p->correlation_id) { bp._flags |= AMQP_BASIC_CORRELATION_ID_FLAG; bp.correlation_id = amqp_cstring_bytes(p->correlation_id); }
        if (p->content_type && *p->content_type) bp.content_type = amqp_cstring_bytes(p->content_type);
        if (p->expiration && *p->expiration) { bp._flags |= AMQP_BASIC_EXPIRATION_FLAG; bp.expiration = amqp_cstring_bytes(p->expiration); }
    }
    payload.len = body_len;
    payload.bytes = (void *)body;
    if (h->chan_dead) reopen_channel_if_needed(h, 0);
    rc = amqp_basic_publish(h->conn, MQC_CHANNEL, amqp_cstring_bytes(exch), amqp_cstring_bytes(key),
                            h->confirm ? 1 : 0, 0, &bp, body_len ? payload : amqp_empty_bytes);
    if (rc != AMQP_STATUS_OK) {
        set_err(h, "basic.publish: %s", amqp_error_string2(rc));
        if (is_fatal_library_error(rc)) { note_connection_dead(h, 0); return MQC_E_BROKER; }
        return MQC_E_LIBRARY;
    }
    if (h->confirm) return wait_confirm(h, exch, key);
    return 0;
}

int mqc_subscribe(int id, const char *queue, int prefetch)
{
    mq_handle *h;
    int rc;
    NEED_HANDLE(h, id);
    clear_err(h);
    if (prefetch < 0) prefetch = 0;
    amqp_basic_qos(h->conn, MQC_CHANNEL, 0, (uint16_t)prefetch, 0);
    rc = check_reply(h, amqp_get_rpc_reply(h->conn), "basic.qos");
    if (rc) {
        /* the broker had already closed the channel (for instance after a publish to a
           missing exchange): answer it and open the channel again so the handle stays usable */
        reopen_channel_if_needed(h, rc);
        return rc;
    }
    amqp_basic_consume(h->conn, MQC_CHANNEL, amqp_cstring_bytes(queue ? queue : ""), amqp_empty_bytes,
                       0, 0, 0, amqp_empty_table);
    rc = check_reply(h, amqp_get_rpc_reply(h->conn), "basic.consume");
    reopen_channel_if_needed(h, rc);
    return rc;
}

int mqc_receive(int id, long timeout_ms, mqc_message *m)
{
    mq_handle *h;
    amqp_rpc_reply_t r;
    amqp_envelope_t env;
    struct timeval tv, *ptv = NULL;
    pending *n;

    NEED_HANDLE(h, id);
    if (!m) { set_err(h, "no message buffer"); return MQC_E_ARGUMENT; }
    memset(m, 0, sizeof(*m));
    clear_err(h);
    if (!h->phead) {            /* nothing set aside by a publisher confirm wait: ask the wire */
        if (timeout_ms >= 0) {
            tv.tv_sec = (time_t)(timeout_ms / 1000);
            tv.tv_usec = (suseconds_t)((timeout_ms % 1000) * 1000);
            ptv = &tv;
        }
        amqp_maybe_release_buffers(h->conn);
        r = amqp_consume_message(h->conn, &env, ptv, 0);
        if (r.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION && r.library_error == AMQP_STATUS_TIMEOUT)
            return MQC_TIMEOUT;
        if (r.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION && r.library_error == AMQP_STATUS_UNEXPECTED_STATE) {
            /* a non-delivery frame arrived: most likely the broker closing the channel */
            amqp_frame_t frame;
            struct timeval none = { 0, 0 };
            if (amqp_simple_wait_frame_noblock(h->conn, &frame, &none) == AMQP_STATUS_OK &&
                frame.frame_type == AMQP_FRAME_METHOD &&
                frame.payload.method.id == AMQP_CHANNEL_CLOSE_METHOD) {
                amqp_channel_close_t *cc = (amqp_channel_close_t *)frame.payload.method.decoded;
                set_err(h, "consume: broker closed channel: %d %.*s (subscriptions are lost)", cc->reply_code,
                        (int)cc->reply_text.len, (char *)cc->reply_text.bytes);
                note_channel_closed(h);
                reopen_channel_if_needed(h, MQC_E_BROKER);
                return MQC_E_BROKER;
            }
            if (frame.frame_type == AMQP_FRAME_METHOD && frame.payload.method.id == AMQP_CONNECTION_CLOSE_METHOD) {
                amqp_connection_close_t *cc = (amqp_connection_close_t *)frame.payload.method.decoded;
                set_err(h, "consume: broker closed connection: %d %.*s", cc->reply_code,
                        (int)cc->reply_text.len, (char *)cc->reply_text.bytes);
                note_connection_dead(h, 1);
                return MQC_E_BROKER;
            }
            set_err(h, "consume: unexpected protocol state");
            return MQC_E_STATE;
        }
        if (r.reply_type != AMQP_RESPONSE_NORMAL)
            return check_reply(h, r, "consume");
        if (queue_pending(h, env.delivery_tag, env.redelivered, env.exchange, env.routing_key,
                          &env.message.properties, env.message.body) != 0) {
            set_err(h, "out of memory");
            amqp_destroy_envelope(&env);
            return MQC_E_LIBRARY;
        }
        amqp_destroy_envelope(&env);
    }

    n = h->phead;
    h->phead = n->next;
    if (!h->phead) h->ptail = NULL;
    m->body = n->body;                 /* ownership moves to the caller */
    m->body_len = n->body_len;
    m->delivery_tag = n->tag;
    m->reply_to = n->reply_to;
    m->correlation_id = n->corr;
    m->exchange = n->exchange;
    m->routing_key = n->key;
    m->redelivered = n->redelivered;
    free(n);
    return 0;
}

void mqc_message_free(mqc_message *m)
{
    if (!m) return;
    free(m->body); free(m->reply_to); free(m->correlation_id); free(m->exchange); free(m->routing_key);
    memset(m, 0, sizeof(*m));
}

int mqc_ack(int id, unsigned long long tag)
{
    mq_handle *h;
    int rc;
    NEED_HANDLE(h, id);
    clear_err(h);
    rc = amqp_basic_ack(h->conn, MQC_CHANNEL, (uint64_t)tag, 0);
    if (rc != AMQP_STATUS_OK) { set_err(h, "basic.ack: %s", amqp_error_string2(rc)); return MQC_E_LIBRARY; }
    return 0;
}

int mqc_nack(int id, unsigned long long tag, int requeue)
{
    mq_handle *h;
    int rc;
    NEED_HANDLE(h, id);
    clear_err(h);
    rc = amqp_basic_nack(h->conn, MQC_CHANNEL, (uint64_t)tag, 0, requeue != 0);
    if (rc != AMQP_STATUS_OK) { set_err(h, "basic.nack: %s", amqp_error_string2(rc)); return MQC_E_LIBRARY; }
    return 0;
}
