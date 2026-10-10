# mq - RabbitMQ for cREXX

Message queueing for [cREXX](https://github.com/adesutherland/crexx) programs
through RabbitMQ (AMQP 0-9-1).

| Part | What it is |
|---|---|
| `rxmq/` | **rxmq**, a native RXPA plugin that wraps `librabbitmq-c`. REXX programs `import rxmq` and call `mqconnect`, `mqpublish`, `mqreceive` and friends. |
| `rabbitmq-crexx-client/` | **mqclient**, a command line client written in cREXX: `send`, `get`, `rpc`, `purge`. |
| `rabbitmq-crexx-server/` | **mqserver**, a request/reply service written in cREXX. It consumes a queue, answers each request on the request's `reply_to` queue and acknowledges it. Change `handle()` to serve your own verbs. |
| `examples/` | `hello`, `worker` and `caller`, small programs to start from. |
| `tests/` | `mq_smoke.crexx`, `mq_pubsub.crexx` and `run_tests.sh`, which runs the whole thing against a live broker. |
| `docs/` | `MQ-Guide-build2.docx`, the user's guide. |

## Build

Needs a cREXX source and build tree, `librabbitmq-dev`, CMake 3.24+ and a RabbitMQ broker to talk to.

```sh
scripts/install-local.sh            # Debian/Ubuntu: broker, cREXX, plugin, all in one go
```

or by hand, with cREXX already built:

```sh
cmake -S . -B build -DCREXX_SOURCE_DIR=/path/to/crexx -DCREXX_BUILD_DIR=/path/to/crexx-build
cmake --build build                 # -> build/bin/rx_rxmq.rxplugin
CREXX_HOME=/opt/crexx tests/run_tests.sh
```

## Running a program

Compile with `rxc`, assemble with `rxas`, run with `rxvm` and name the plugin
after the `.rxbin`. The plugin is found in the working directory or via
`--provider-path`:

```sh
rxc -i /opt/crexx/bin -i build/bin -o mqserver rabbitmq-crexx-server/mqserver.crexx
rxas -o mqserver mqserver
rxvm mqserver.rxbin rx_rxmq /opt/crexx/bin/library -a jobs
```

Everything after `-a` goes to the program. The same pattern runs `mqclient`:

```sh
rxvm mqclient.rxbin rx_rxmq /opt/crexx/bin/library -a rpc jobs upper hello world    # -> HELLO WORLD
rxvm mqclient.rxbin rx_rxmq /opt/crexx/bin/library -a send inbox some text
rxvm mqclient.rxbin rx_rxmq /opt/crexx/bin/library -a get inbox
```

Options (both programs): `-b broker`, `-P port`, `-u user`, `-p password`, `-v vhost`;
`mqclient -t ms` sets the wait time, `mqserver -n count` stops after that many
requests and `-V` logs each request.

## The rxmq API

```rexx
options levelb
import rxmq
```

Every call returns an `.int`: `0` or a positive handle on success, a negative
number on failure. `mqerror(handle)` returns the text of the last error
(`mqerror(0)` after a failed `mqconnect`).

| Call | Purpose |
|---|---|
| `mqconnect(broker, port, user, password, vhost)` | Connect and open a channel. Returns a handle (1..16). An empty broker, user or vhost and a port of 0 use `localhost`, `guest`, `/` and 5672; the password has no default, pass `"guest"`. |
| `mqdisconnect(handle)` | Close channel and connection. |
| `mqqueue(handle, name, flags)` | Declare a queue. Flags: `DURABLE EXCLUSIVE AUTODELETE PASSIVE`. `PASSIVE` only checks that it exists. |
| `mqtempqueue(handle)` | Declare an exclusive, auto-delete queue with a broker-generated name and return the name (`""` on error). Use it as a reply queue. |
| `mqexchange(handle, name, type, flags)` | Declare an exchange (`direct`, `fanout`, `topic`, ...). Flags: `DURABLE AUTODELETE`. |
| `mqbind(handle, queue, exchange, routingkey)` | Bind a queue to an exchange. |
| `mqpublish(handle, exchange, routingkey, body, props)` | Publish. Exchange `""` is the default exchange: the routing key is then a queue name. Returns `-11` if (after `mqconfirm`) no queue could take the message. |
| `mqconfirm(handle)` | Switch the connection to publisher confirms: `mqpublish` then waits for the broker and reports unroutable (`-11`) or refused (`-6`) messages instead of returning 0. |
| `mqsubscribe(handle, queue, prefetch)` | Start consuming with manual acknowledgement; `prefetch` limits unacknowledged messages. |
| `mqreceive(handle, timeout_ms, msg)` | Wait for a message. `0` = got one, `1` = timeout, negative = error. `timeout_ms < 0` waits forever. |
| `mqack(handle, tag)` / `mqnack(handle, tag, requeue)` | Acknowledge, or reject with or without requeueing. |
| `mqpurge(handle, queue)` / `mqdelete(handle, queue)` | Empty or delete a queue. `mqpurge` returns the number of messages removed. |
| `mqflags("DEBUG")` | Trace the plugin; `NDEBUG` switches it off. |

`props` for `mqpublish` is a blank-separated list: `persistent`, `reply_to=<queue>`,
`correlation_id=<id>`, `content_type=<type>`, `expiration=<ms>`. Values cannot contain blanks.

`msg` for `mqreceive` is a `.string[]` that the call fills in:

| Element | Content |
|---|---|
| `msg[1]` | message body |
| `msg[2]` | delivery tag (pass it to `mqack`/`mqnack`) |
| `msg[3]` | `reply_to` |
| `msg[4]` | `correlation_id` |
| `msg[5]` | exchange |
| `msg[6]` | routing key |
| `msg[7]` | `1` if redelivered, else `0` |

A minimal worker:

```rexx
options levelb
import rxmq

h = mqconnect("localhost", 5672, "guest", "guest", "/")
call mqqueue h, "jobs", "DURABLE"
call mqsubscribe h, "jobs", 1
msg = .string[]
do while mqreceive(h, -1, msg) = 0
  say "got:" msg[1]
  call mqack h, msg[2]
end
```

## Behaviour worth knowing

- **Text bodies.** Bodies are REXX strings; a body containing a NUL byte is cut at the NUL.
- **No heartbeats.** The connection is opened with heartbeats off so a program may sit idle between calls. A broker or network failure shows up as an error from the next call.
- **Broker-side channel errors are survivable.** If the broker closes the channel (for example `mqpurge` of a missing queue gives `404 NOT_FOUND`), the call returns an error, the channel is reopened and the handle stays usable. Subscriptions on that channel are lost and must be made again.
- **Publishing is fire-and-forget until you call `mqconfirm`.** Without confirms a message for a queue that does not exist is dropped and `mqpublish` still returns 0. `mqclient` and `mqserver` switch confirms on.
- **A deleted queue silently ends a subscription.** The broker does not tell this client. `mqserver` therefore checks every ~5 idle seconds that its queue still exists and re-creates and resubscribes if not.
- **Reserved word.** `queue` is a keyword in Level B; do not use it as a variable name.
- **Handles.** At most 16 connections per process, one channel each.

## Tests

`tests/run_tests.sh` builds the programs and checks, against a live broker: the
plugin round trip (publish, properties, subscribe, receive, ack, timeout), the
server with RPC verbs and self-stop, `send`/`get`, timeouts and exit codes, an
unreachable broker, broker-side errors, and server recovery after its queue is
deleted. It runs 27 checks and takes about 30 seconds.
