package io.rsocket.transport.netty.server;

import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.TransportHeaderAware;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.NettyPipeline.SendOptions;
import reactor.ipc.netty.http.server.HttpServer;

/**
 * Extended class for RSocket's {@link WebsocketServerTransport}. The point of it is to control
 * channel flushing behavior. See class {@link SendOptions} for more details.
 */
public class ExtendedWebsocketServerTransport
  implements ServerTransport<NettyContextCloseable>, TransportHeaderAware {

  private final HttpServer server;

  private Supplier<Map<String, String>> transportHeaders = Collections::emptyMap;

  /**
   * Constructor for this websocket server transport.
   *
   * @param server http server
   */
  public ExtendedWebsocketServerTransport(HttpServer server) {
    this.server = server;
  }

  @Override
  public void setTransportHeaders(Supplier<Map<String, String>> transportHeaders) {
    this.transportHeaders =
      Objects.requireNonNull(transportHeaders, "transportHeaders must not be null");
  }

  @Override
  public Mono<NettyContextCloseable> start(ConnectionAcceptor acceptor) {
    Objects.requireNonNull(acceptor, "acceptor must not be null");

    return server
      .newHandler(
        (request, response) -> {
          transportHeaders.get().forEach(response::addHeader);
          response.options(SendOptions::flushOnEach);
          // response.options(SendOptions::flushOnBoundary);
          return response.sendWebsocket(WebsocketRouteTransport.newHandler(acceptor));
        })
      .map(NettyContextCloseable::new);
  }
}
