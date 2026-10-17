# 0002 — Close accepted connections on server transport stop

Status: Proposed

## Context

`WebsocketGatewayRequestReleaseTest.remoteConnectionLossReleasesRequestDataOnce` failed on CI in
three of four consecutive runs: after the node hosting the service was closed, the gateway's
in-flight stream to it never errored. A passing run shows the error within 1 ms of the close.

The failure was reproduced locally on four pinned, saturated cores. At the moment of failure:

- the closed node's rsocket event-loop group was fully terminated;
- the gateway's `RSocketRequester` to that node was not disposed (`availability=1.0`);
- the node's accepted channel was still `open=true active=true registered=true`, on an event loop
  that reported `isTerminated=true`;
- `ss -tan` showed the TCP connection `ESTABLISHED` on both ends: no FIN was ever sent.

The node never closed the accepted socket. `RSocketServerTransport.stop()` disposed only the
listening channel (Reactor Netty's `DisposableServer.dispose()` closes the server socket, not the
accepted children), and `RSocketServiceTransport.stop()` relied on
`eventLoopGroup.shutdownGracefully(0, 0, MILLISECONDS)` to close every channel registered on the
loops. That held with Netty 4.1 (`NioEventLoop.run()` calls `closeAll()` whenever the loop is
shutting down, before `confirmShutdown()`). It does not hold with Netty 4.2:

```java
// io.netty.channel.SingleThreadIoEventLoop#run (4.2.18.Final, unchanged on the 4.2 branch)
do {
    runIo();
    if (isShuttingDown()) {
        ioHandler.prepareToDestroy();   // this is what closes the registered channels
    }
    runAllTasks(maxTaskProcessingQuantumNs);
} while (!confirmShutdown() && !canSuspend());
```

If `shutdownGracefully` is requested while the loop is inside `runAllTasks(...)`, the
`isShuttingDown()` check has already been passed, and `confirmShutdown()` — with a zero quiet
period — returns `true` on its first call. The loop exits, `cleanup()` closes the selector, and
`prepareToDestroy()` is never called: every channel registered on that loop stays open. Nothing is
logged. A standalone program against Netty 4.2.18 (one registered channel, shutdown requested while
a task is running) left the channel open 20 times out of 20 with quiet period 0, and 0 times out of
20 with any non-zero quiet period.

The race needs the loop to be busy with tasks exactly when the node is closed. That is why the
failure appeared only on the first rsocket connection in a JVM (class loading makes the first
connection's tasks slow) and only on loaded CI runners.

The consequence for callers is not test-only: a peer whose node shuts down this way keeps its
in-flight requests open until rsocket keepalive gives up on the connection.

## Alternatives considered

1. **Non-zero quiet period only** (`shutdownGracefully()` with Netty's defaults). Makes
   `confirmShutdown()` return `false` on the first pass whenever a task ran recently, so the next
   iteration runs `prepareToDestroy()`. Closes the common window, but still relies on the event
   loop shutdown to close connections, and a narrower window remains (shutdown requested right
   after a `runAllTasks` that ran nothing, with the last task older than the quiet period).
2. **Dispose the accepted `RSocket`s from the acceptor.** `SocketAcceptor.accept` receives the
   sending `RSocket`; disposing it sends a stream-0 `ERROR(CONNECTION_ERROR, "Disposed")` and then
   closes. Explicit, but it changes what the peer observes: `ConnectionErrorException` instead of
   the `ClosedChannelException` a dropped connection produces today, which `RSocketClientChannel`
   maps to `ConnectionClosedException`.
3. **Reactor Netty `channelGroup` + `disposeNow()`.** Closes children on server dispose, but the
   server transport factory is pluggable (`RSocketServerTransportFactory`) and rsocket's
   `CloseableChannel` does not expose `disposeNow()`.
4. **Track the source `DuplexConnection`s via rsocket's `InterceptorRegistry.forConnection` and
   dispose them in `stop()`.** rsocket passes every accepted transport connection through the
   registered `DuplexConnectionInterceptor` as `Type.SOURCE`. Disposing a `DuplexConnection`
   closes the socket, so the peer sees the same `ClosedChannelException` as before. Works with any
   server transport factory.

## Decision

Both of the following, in `services-transport-rsocket`:

- `RSocketServerTransport` registers a `DuplexConnectionInterceptor` that records each `SOURCE`
  connection and removes it on `onClose()`. `stop()` disposes the recorded connections before
  disposing the listening channel. Connections are closed by the transport, by construction, not
  as a side effect of thread-pool shutdown.
- `RSocketServiceTransport.stop()` shuts the event-loop group down with Netty's default
  `shutdownGracefully()` (2 s quiet period, 15 s timeout) instead of `(0, 0)`. The close tasks the
  server and client transports queue need the loops to keep running tasks for a moment, and the
  default quiet period also makes Netty's own `prepareToDestroy()` pass run for anything else
  still registered. The call is not awaited, so `Microservices.close()` does not get slower; the
  loop threads linger for up to the quiet period.

## Consequences

- A closed node drops its accepted connections immediately; peers' in-flight requests fail with
  `ConnectionClosedException` as they do when a node dies, instead of hanging.
- Event-loop threads of a closed node live up to two more seconds (daemon threads).
- `RSocketServerTransport` holds a reference to every live accepted connection until it closes.
- The Netty 4.2 behaviour is upstream and still present on the `4.2` branch; this change makes the
  transport independent of it rather than waiting for a fix.

Evidence: `RSocketServiceTransportTest.test_closing_node_drops_its_accepted_connections` (caller
observes `ConnectionClosedException` after the node closes) and
`WebsocketGatewayRequestReleaseTest.remoteConnectionLossReleasesRequestDataOnce` (the original CI
failure, end to end through the gateway).

## References

- Netty 4.2 `io.netty.channel.SingleThreadIoEventLoop#run` and
  `io.netty.channel.nio.NioIoHandler#prepareToDestroy` (4.2.18.Final; the ordering is the same on
  the `4.2` branch head). Netty 4.1 `io.netty.channel.nio.NioEventLoop#run` / `closeAll()` for the
  behaviour this transport previously relied on. Primary sources.
- Reactor Netty 1.2.8 `reactor.netty.transport.ServerTransport.DisposableBind#dispose` (closes the
  server channel only). Primary source.
- rsocket-java 1.1.5 `io.rsocket.plugins.InterceptorRegistry#forConnection`,
  `io.rsocket.core.RSocketServer` (`Type.SOURCE` interception of accepted connections),
  `io.rsocket.core.RSocketRequester#dispose` (sends `ERROR` before closing; why alternative 2 was
  not taken). Primary sources.
- Reproduction: diagnostics captured on the failing test (event-loop state, channel state,
  `ss -tan`), and a 20-attempt standalone Netty program per quiet period. Measured, this repository.
