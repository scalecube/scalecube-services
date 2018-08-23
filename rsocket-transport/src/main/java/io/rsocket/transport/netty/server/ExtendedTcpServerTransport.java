package io.rsocket.transport.netty.server;

import io.rsocket.transport.netty.RSocketLengthCodec;
import io.rsocket.transport.netty.TcpDuplexConnection;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.NettyPipeline.SendOptions;
import reactor.ipc.netty.tcp.TcpServer;

public class ExtendedTcpServerTransport
    implements io.rsocket.transport.ServerTransport<NettyContextCloseable> {

  private final TcpServer server;

  public ExtendedTcpServerTransport(TcpServer server) {
    this.server = server;
  }

  @Override
  public Mono<NettyContextCloseable> start(ConnectionAcceptor acceptor) {
    return server
        .newHandler(
            (in, out) -> {
              in.context().addHandler(new RSocketLengthCodec());
              out.options(SendOptions::flushOnEach);
              // out.options(SendOptions::flushOnBoundary);
              TcpDuplexConnection connection = new TcpDuplexConnection(in, out, in.context());
              acceptor.apply(connection).subscribe();
              return out.neverComplete();
            })
        .map(NettyContextCloseable::new);
  }
}
