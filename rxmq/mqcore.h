/*
 * mqcore - AMQP 0-9-1 client core on librabbitmq-c, in plain C: no cREXX, no REXX strings.
 *
 * Used by the cREXX plugin rxmq (rxmq/rxmq.c) and meant for any C program that needs the same
 * logic (connect, channel reopen after a broker-side close, publisher confirms, a delivery queue,
 * ack/nack, "connection is dead" handling).
 *
 * A connection is an integer handle 1..MQC_MAX_HANDLES; each owns one TCP connection and one
 * channel.  Functions return 0 (or a handle / count) on success and a negative MQC_E_* code on
 * failure; mqc_error() gives the text.  Message bodies are a pointer plus a length and are
 * binary safe: NUL bytes survive in both directions.  All other strings are NUL terminated.
 *
 * Single threaded: calls on one handle must not overlap.  Needs POSIX select/time (librabbitmq-c).
 */
#ifndef MQCORE_H
#define MQCORE_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MQC_MAX_HANDLES 16

/* status codes */
#define MQC_E_BADHANDLE  (-1)   /* not an open connection */
#define MQC_E_SOCKET     (-2)   /* no socket */
#define MQC_E_OPEN       (-3)   /* broker not reachable */
#define MQC_E_LOGIN      (-4)   /* login refused or no such vhost */
#define MQC_E_CHANNEL    (-5)   /* channel could not be opened */
#define MQC_E_BROKER     (-6)   /* broker closed channel/connection, refused, or connection lost */
#define MQC_E_LIBRARY    (-7)   /* librabbitmq-c error, out of memory */
#define MQC_E_ARGUMENT   (-8)   /* unacceptable argument */
#define MQC_E_NOSPACE    (-9)   /* all handles in use */
#define MQC_E_STATE      (-10)  /* unexpected protocol state */
#define MQC_E_UNROUTABLE (-11)  /* confirms on: accepted by the broker but no queue took it */
#define MQC_TIMEOUT      1      /* mqc_receive: nothing arrived in time */

/* flags for mqc_exchange / mqc_queue */
#define MQC_DURABLE    1
#define MQC_EXCLUSIVE  2
#define MQC_AUTODELETE 4
#define MQC_PASSIVE    8

/* message properties for mqc_publish; NULL strings mean "not set" */
typedef struct mqc_props {
    int         persistent;
    const char *reply_to;
    const char *correlation_id;
    const char *content_type;       /* default text/plain */
    const char *expiration;         /* milliseconds, as text */
} mqc_props;

/* a received message; free with mqc_message_free */
typedef struct mqc_message {
    char              *body;            /* body_len bytes, plus a terminating NUL that is not counted */
    size_t             body_len;
    unsigned long long delivery_tag;    /* for mqc_ack / mqc_nack */
    char              *reply_to;        /* "" when absent */
    char              *correlation_id;  /* "" when absent */
    char              *exchange;
    char              *routing_key;
    int                redelivered;
} mqc_message;

void mqc_set_debug(int on);
const char *mqc_error(int handle);      /* handle 0: last error of any call (e.g. a failed connect) */
/* record a caller-detected error text on a handle (0 = global); for argument checks in wrappers */
void mqc_set_error(int handle, const char *text);

int  mqc_connect(const char *host, int port, const char *user, const char *password, const char *vhost);
int  mqc_disconnect(int handle);
int  mqc_confirm(int handle);           /* publisher confirms on; cannot be switched off */
int  mqc_exchange(int handle, const char *name, const char *type, int flags);
int  mqc_queue(int handle, const char *name, int flags);
int  mqc_tempqueue(int handle, char *name, size_t namelen);
int  mqc_purge(int handle, const char *queue);          /* returns the number purged */
int  mqc_delete(int handle, const char *queue);
int  mqc_bind(int handle, const char *queue, const char *exchange, const char *routing_key);
int  mqc_publish(int handle, const char *exchange, const char *routing_key,
                 const void *body, size_t body_len, const mqc_props *props);
int  mqc_subscribe(int handle, const char *queue, int prefetch);
int  mqc_receive(int handle, long timeout_ms, mqc_message *msg);   /* 0, MQC_TIMEOUT or <0 */
void mqc_message_free(mqc_message *msg);
int  mqc_ack(int handle, unsigned long long delivery_tag);
int  mqc_nack(int handle, unsigned long long delivery_tag, int requeue);

#ifdef __cplusplus
}
#endif
#endif
