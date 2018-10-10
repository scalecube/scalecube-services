package io.scalecube.gateway.rsocket.websocket;

import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.TransportHeaderAware;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.transport.netty.server.WebsocketRouteTransport;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.NettyContext;
import reactor.ipc.netty.http.server.HttpServer;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;

public class RSocketWebsocketServerTransport
    implements ServerTransport<NettyContextCloseable>, TransportHeaderAware {

  private final HttpServer server;

  private Supplier<Map<String, String>> transportHeaders = Collections::emptyMap;

  public RSocketWebsocketServerTransport(HttpServer server) {
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

              response.options(
                  sendOptions -> sendOptions.flushOnEach(false)); // set flush immediately

              try {
                Method method =
                    WebsocketRouteTransport.class.getDeclaredMethod(
                        "newHandler", ConnectionAcceptor.class);
                method.setAccessible(true);
                BiFunction<
                        ? super WebsocketInbound,
                        ? super WebsocketOutbound,
                        ? extends Publisher<Void>>
                    handler =
                        (BiFunction<
                                ? super WebsocketInbound,
                                ? super WebsocketOutbound,
                                ? extends Publisher<Void>>)
                            method.invoke(null, acceptor);

                return response.sendWebsocket(handler);
              } catch (Exception e) {
                throw Exceptions.propagate(e);
              }
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
