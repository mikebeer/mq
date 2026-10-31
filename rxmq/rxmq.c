/*
 * rxmq - RabbitMQ (AMQP 0-9-1) client plugin for cREXX / RXPA
 *
 * The AMQP logic lives in mqcore.c (plain C, see mqcore.h); this file only turns REXX arguments into
 * calls of it.  Every REXX-visible connection is an integer handle (1..16).  All procedures return
 * an .int status:
 *
 *      0 (or a positive handle)  success
 *      1                         mqreceive: timeout, no message
 *     <0                         failure; mqerror(handle) gives the text
 *
 * Message bodies are passed as REXX strings, i.e. text.  A body containing a NUL byte is truncated
 * at the NUL when received.  (mqcore itself is binary safe.)
 */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>

#include "mqcore.h"
#include "crexxpa.h"

/* has the (case-insensitive) word in a space separated flag string? */
static int has_flag(const char *flags, const char *word)
{
    size_t wl = strlen(word);
    const char *p = flags;
    if (!flags) return 0;
    while (*p) {
        size_t n = 0;
        while (*p && isspace((unsigned char)*p)) p++;
        while (p[n] && !isspace((unsigned char)p[n])) n++;
        if (n == wl && strncasecmp(p, word, n) == 0) return 1;
        p += n;
    }
    return 0;
}

static int flag_bits(const char *flags)
{
    return (has_flag(flags, "DURABLE") ? MQC_DURABLE : 0) | (has_flag(flags, "EXCLUSIVE") ? MQC_EXCLUSIVE : 0) |
           (has_flag(flags, "AUTODELETE") ? MQC_AUTODELETE : 0) | (has_flag(flags, "PASSIVE") ? MQC_PASSIVE : 0);
}

/* mqconnect(broker, port, user, password, vhost) -> handle | <0 */
PROCEDURE(mqconnect) {
    RETURNINTX(mqc_connect(GETSTRING(ARG0), (int)GETINT(ARG1), GETSTRING(ARG2), GETSTRING(ARG3), GETSTRING(ARG4)));
    ENDPROC
}

/* mqdisconnect(handle) -> 0 | <0 */
PROCEDURE(mqdisconnect) {
    RETURNINTX(mqc_disconnect((int)GETINT(ARG0)));
    ENDPROC
}

/* mqexchange(handle, name, type, flags) -> 0 | <0   flags: DURABLE AUTODELETE */
PROCEDURE(mqexchange) {
    RETURNINTX(mqc_exchange((int)GETINT(ARG0), GETSTRING(ARG1), GETSTRING(ARG2), flag_bits(GETSTRING(ARG3))));
    ENDPROC
}

/* mqqueue(handle, name, flags) -> 0 | <0   flags: DURABLE EXCLUSIVE AUTODELETE PASSIVE */
PROCEDURE(mqqueue) {
    RETURNINTX(mqc_queue((int)GETINT(ARG0), GETSTRING(ARG1), flag_bits(GETSTRING(ARG2))));
    ENDPROC
}

/* mqtempqueue(handle) -> broker generated name of an exclusive auto-delete queue, "" on error */
PROCEDURE(mqtempqueue) {
    char name[512];
    if (mqc_tempqueue((int)GETINT(ARG0), name, sizeof(name)) != 0) RETURNSTRX("");
    RETURNSTR(name);
    ENDPROC
}

/* mqpurge(handle, queue) -> number of messages purged | <0 */
PROCEDURE(mqpurge) {
    RETURNINTX(mqc_purge((int)GETINT(ARG0), GETSTRING(ARG1)));
    ENDPROC
}

/* mqdelete(handle, queue) -> 0 | <0 */
PROCEDURE(mqdelete) {
    RETURNINTX(mqc_delete((int)GETINT(ARG0), GETSTRING(ARG1)));
    ENDPROC
}

/* mqbind(handle, queue, exchange, routingkey) -> 0 | <0 */
PROCEDURE(mqbind) {
    RETURNINTX(mqc_bind((int)GETINT(ARG0), GETSTRING(ARG1), GETSTRING(ARG2), GETSTRING(ARG3)));
    ENDPROC
}

/* mqconfirm(handle) -> 0 | <0
 * Switch the connection to publisher confirms: from now on mqpublish waits until the broker has
 * accepted the message, and fails with -11 when no queue could take it.  Cannot be switched off. */
PROCEDURE(mqconfirm) {
    RETURNINTX(mqc_confirm((int)GETINT(ARG0)));
    ENDPROC
}

/*
 * mqpublish(handle, exchange, routingkey, body, props) -> 0 | <0
 *
 * exchange "" publishes through the default exchange, i.e. routingkey is a queue name.
 * props is a blank separated list: persistent  reply_to=Q  correlation_id=ID  content_type=T  expiration=MS
 * (values cannot contain blanks).
 */
PROCEDURE(mqpublish) {
    int id = (int)GETINT(ARG0);
    const char *exch = GETSTRING(ARG1);
    const char *key  = GETSTRING(ARG2);
    const char *body = GETSTRING(ARG3);
    const char *props = GETSTRING(ARG4);
    mqc_props p;
    char *pcopy, *tok, *save = NULL;
    char text[256];
    int rc;

    memset(&p, 0, sizeof(p));
    pcopy = (char *)malloc((props ? strlen(props) : 0) + 1);
    if (!pcopy) { mqc_set_error(id, "out of memory"); RETURNINTX(MQC_E_LIBRARY); }
    strcpy(pcopy, props ? props : "");
    for (tok = strtok_r(pcopy, " \t\r\n", &save); tok; tok = strtok_r(NULL, " \t\r\n", &save)) {
        char *eq = strchr(tok, '=');
        if (eq) {
            *eq++ = '\0';
            if (!strcasecmp(tok, "reply_to")) p.reply_to = eq;
            else if (!strcasecmp(tok, "correlation_id")) p.correlation_id = eq;
            else if (!strcasecmp(tok, "content_type")) p.content_type = eq;
            else if (!strcasecmp(tok, "expiration")) p.expiration = eq;
            else { snprintf(text, sizeof(text), "unknown property '%.100s'", tok); mqc_set_error(id, text); free(pcopy); RETURNINTX(MQC_E_ARGUMENT); }
        } else if (!strcasecmp(tok, "persistent")) {
            p.persistent = 1;
        } else {
            snprintf(text, sizeof(text), "unknown property '%.100s'", tok);
            mqc_set_error(id, text);
            free(pcopy);
            RETURNINTX(MQC_E_ARGUMENT);
        }
    }
    rc = mqc_publish(id, exch, key, body ? body : "", body ? strlen(body) : 0, &p);
    free(pcopy);
    RETURNINTX(rc);
    ENDPROC
}

/* mqsubscribe(handle, queue, prefetch) -> 0 | <0   (manual acknowledgement) */
PROCEDURE(mqsubscribe) {
    RETURNINTX(mqc_subscribe((int)GETINT(ARG0), GETSTRING(ARG1), (int)GETINT(ARG2)));
    ENDPROC
}

/*
 * mqreceive(handle, timeout_ms, msg) -> 0 | 1 | <0
 *   0  a message arrived; msg (a .string[]) is filled:
 *        msg[1] body            msg[2] delivery tag    msg[3] reply_to
 *        msg[4] correlation_id  msg[5] exchange        msg[6] routing key
 *        msg[7] redelivered (0/1)
 *   1  timeout (msg untouched)
 * timeout_ms < 0 waits forever.
 */
PROCEDURE(mqreceive) {
    mqc_message m;
    char tag[32];
    int rc = mqc_receive((int)GETINT(ARG0), (long)GETINT(ARG1), &m);
    if (rc != 0) RETURNINTX(rc);
    snprintf(tag, sizeof(tag), "%llu", m.delivery_tag);
    SETARRAYHI(ARG2, 7);
    SETSARRAY(ARG2, 0, m.body);          /* a REXX string is text: cut at a NUL byte */
    SETSARRAY(ARG2, 1, tag);
    SETSARRAY(ARG2, 2, m.reply_to);
    SETSARRAY(ARG2, 3, m.correlation_id);
    SETSARRAY(ARG2, 4, m.exchange);
    SETSARRAY(ARG2, 5, m.routing_key);
    SETSARRAY(ARG2, 6, m.redelivered ? "1" : "0");
    mqc_message_free(&m);
    RETURNINTX(0);
    ENDPROC
}

/* mqack(handle, tag) -> 0 | <0 */
PROCEDURE(mqack) {
    const char *tag = GETSTRING(ARG1);
    RETURNINTX(mqc_ack((int)GETINT(ARG0), strtoull(tag ? tag : "0", NULL, 10)));
    ENDPROC
}

/* mqnack(handle, tag, requeue) -> 0 | <0   requeue: 1 = put back on the queue, 0 = discard/dead-letter */
PROCEDURE(mqnack) {
    const char *tag = GETSTRING(ARG1);
    RETURNINTX(mqc_nack((int)GETINT(ARG0), strtoull(tag ? tag : "0", NULL, 10), (int)GETINT(ARG2)));
    ENDPROC
}

/* mqerror(handle) -> text of the last error; handle 0 = last error of any connection attempt */
PROCEDURE(mqerror) {
    RETURNSTRX(mqc_error((int)GETINT(ARG0)));
    ENDPROC
}

/* mqflags(flags) -> 0     flags: DEBUG NDEBUG */
PROCEDURE(mqflags) {
    const char *flags = GETSTRING(ARG0);
    if (has_flag(flags, "DEBUG")) mqc_set_debug(1);
    if (has_flag(flags, "NDEBUG")) mqc_set_debug(0);
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
    ADDPROC(mqconfirm,    "rxmq.mqconfirm",    "b", ".int",    "handle=.int");
    ADDPROC(mqpublish,    "rxmq.mqpublish",    "b", ".int",    "handle=.int,exchange=.string,routingkey=.string,body=.string,props=.string");
    ADDPROC(mqsubscribe,  "rxmq.mqsubscribe",  "b", ".int",    "handle=.int,queue=.string,prefetch=.int");
    ADDPROC(mqreceive,    "rxmq.mqreceive",    "b", ".int",    "handle=.int,timeout=.int,expose msg=.string[]");
    ADDPROC(mqack,        "rxmq.mqack",        "b", ".int",    "handle=.int,tag=.string");
    ADDPROC(mqnack,       "rxmq.mqnack",       "b", ".int",    "handle=.int,tag=.string,requeue=.int");
    ADDPROC(mqerror,      "rxmq.mqerror",      "b", ".string", "handle=.int");
ENDLOADFUNCS
