package io.scalecube.services.transport.rsocket;

import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.RSocketLengthCodec;
import io.rsocket.transport.netty.TcpDuplexConnection;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import java.lang.reflect.Constructor;
import java.util.Objects;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.NettyContext;
import reactor.ipc.netty.tcp.TcpServer;

public final class RSocketTcpServerTransport implements ServerTransport<NettyContextCloseable> {

  private final TcpServer server;

  public RSocketTcpServerTransport(TcpServer server) {
    this.server = server;
  }

  @Override
  public Mono<NettyContextCloseable> start(ConnectionAcceptor acceptor) {
    Objects.requireNonNull(acceptor, "acceptor must not be null");

    return server
        .newHandler(
            (in, out) -> {
              in.context().addHandler(new RSocketLengthCodec());

              TcpDuplexConnection connection = new TcpDuplexConnection(in, out, in.context());
              acceptor.apply(connection).subscribe();

              out.options(sendOptions -> sendOptions.flushOnEach(false)); // set flush immediately

              return out.neverComplete();
            })
        .map(
            context -> {
              try {
                Constructor<NettyContextCloseable> constructor =
                    NettyContextCloseable.class.getDeclaredConstructor(NettyContext.class);
                constructor.setAccessible(true);
                return constructor.newInstance(context);
              } catch (Exception e) {
                throw Exceptions.propagate(e);
              }
            });
  }
}
