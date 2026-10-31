/* mqcore_test - exercises mqcore.c from plain C (no cREXX): binary-safe bodies, confirms, errors.
 * Needs a broker on localhost with guest/guest.  Exit code 0 = pass. */
#include <stdio.h>
#include <string.h>
#include "mqcore.h"

static int fails = 0;
#define CHECK(cond, what) do { if (!(cond)) { printf("FAIL: %s (%s)\n", what, mqc_error(h)); fails++; } } while (0)

int main(void)
{
    static const char bin[] = { 'a', 0, 'b', (char)0xff, 0, 'c' };   /* 6 bytes with NULs */
    mqc_message m;
    mqc_props p;
    char tmp[256];
    int h, rc, i;

    h = mqc_connect("localhost", 5672, "guest", "guest", "/");
    if (h < 1) { printf("FAIL: connect: %s\n", mqc_error(0)); return 1; }

    CHECK(mqc_queue(h, "rxmq.core.test", MQC_DURABLE) == 0, "declare");
    mqc_purge(h, "rxmq.core.test");
    CHECK(mqc_confirm(h) == 0, "confirm");

    memset(&p, 0, sizeof(p));
    p.correlation_id = "c-1";
    p.reply_to = "somewhere";
    CHECK(mqc_publish(h, "", "rxmq.core.test", bin, sizeof(bin), &p) == 0, "publish binary");
    CHECK(mqc_publish(h, "", "rxmq.core.test", "", 0, NULL) == 0, "publish empty body");
    CHECK(mqc_publish(h, "", "rxmq.core.nobody", "x", 1, NULL) == MQC_E_UNROUTABLE, "unroutable");
    CHECK(mqc_publish(h, "rxmq.core.noexch", "k", "x", 1, NULL) == MQC_E_BROKER, "missing exchange");

    CHECK(mqc_subscribe(h, "rxmq.core.test", 1) == 0, "subscribe");
    memset(&m, 0, sizeof(m));
    rc = mqc_receive(h, 3000, &m);
    CHECK(rc == 0, "receive binary");
    if (rc == 0) {
        CHECK(m.body_len == sizeof(bin) && memcmp(m.body, bin, sizeof(bin)) == 0, "binary body intact (NULs kept)");
        CHECK(strcmp(m.correlation_id, "c-1") == 0 && strcmp(m.reply_to, "somewhere") == 0, "properties");
        CHECK(strcmp(m.routing_key, "rxmq.core.test") == 0, "routing key");
        CHECK(mqc_ack(h, m.delivery_tag) == 0, "ack");
        mqc_message_free(&m);
    }
    rc = mqc_receive(h, 3000, &m);
    CHECK(rc == 0 && m.body_len == 0, "empty body received as length 0");
    if (rc == 0) { mqc_ack(h, m.delivery_tag); mqc_message_free(&m); }
    CHECK(mqc_receive(h, 300, &m) == MQC_TIMEOUT, "timeout");

    /* a body larger than one AMQP frame (131072 bytes) with every byte value */
    {
        static char big[300000];
        for (i = 0; i < (int)sizeof(big); i++) big[i] = (char)(i * 7);
        CHECK(mqc_publish(h, "", "rxmq.core.test", big, sizeof(big), NULL) == 0, "publish 300000 bytes");
        rc = mqc_receive(h, 5000, &m);
        CHECK(rc == 0 && m.body_len == sizeof(big) && memcmp(m.body, big, sizeof(big)) == 0, "300000 byte body intact");
        if (rc == 0) { mqc_ack(h, m.delivery_tag); mqc_message_free(&m); }
    }

    CHECK(mqc_tempqueue(h, tmp, sizeof(tmp)) == 0 && strncmp(tmp, "amq.gen-", 8) == 0, "tempqueue");
    CHECK(mqc_publish(h, "", "rxmq.core.test", "x", 1, NULL) == 0, "handle usable after errors");
    mqc_purge(h, "rxmq.core.test");
    CHECK(mqc_delete(h, "rxmq.core.test") == 0, "delete");
    CHECK(mqc_disconnect(h) == 0, "disconnect");
    CHECK(mqc_disconnect(h) == MQC_E_BADHANDLE, "second disconnect");

    if (fails == 0) printf("PASS: mqcore\n");
    return fails ? 1 : 0;
}
