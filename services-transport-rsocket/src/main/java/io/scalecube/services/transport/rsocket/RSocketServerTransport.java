package io.scalecube.services.transport.rsocket;

import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.transport.api.Address;
import io.scalecube.services.transport.api.ServerTransport;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpServer;

/** RSocket server transport implementation. */
public class RSocketServerTransport implements ServerTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServerTransport.class);

  private final ServiceMessageCodec codec;
  private final LoopResources loopResources;

  private CloseableChannel server; // calculated
  private List<Connection> connections = new CopyOnWriteArrayList<>(); // calculated

  /**
   * Constructor for this server transport.
   *
   * @param codec message codec
   * @param loopResources server loop resources
   */
  public RSocketServerTransport(ServiceMessageCodec codec, LoopResources loopResources) {
    this.codec = codec;
    this.loopResources = loopResources;
  }

  @Override
  public Address address() {
    InetSocketAddress address = server.address();
    return Address.create(address.getHostString(), address.getPort());
  }

  @Override
  public Mono<ServerTransport> bind(int port, ServiceMethodRegistry methodRegistry) {
    return Mono.defer(
        () -> {
          TcpServer tcpServer =
              TcpServer.create()
                  .runOn(loopResources)
                  .addressSupplier(() -> new InetSocketAddress(port))
                  .doOnConnection(
                      connection -> {
                        LOGGER.info("Accepted connection on {}", connection.channel());
                        connection.onDispose(
                            () -> {
                              LOGGER.info("Connection closed on {}", connection.channel());
                              connections.remove(connection);
                            });
                        connections.add(connection);
                      });

          return RSocketFactory.receive()
              .frameDecoder(
                  frame ->
                      ByteBufPayload.create(
                          frame.sliceData().retain(), frame.sliceMetadata().retain()))
              .acceptor(new RSocketServiceAcceptor(codec, methodRegistry))
              .transport(() -> TcpServerTransport.create(tcpServer))
              .start()
              .doOnSuccess(channel -> this.server = channel)
              .thenReturn(this);
        });
  }

  @Override
  public Mono<Void> stop() {
    return Flux //
        .concatDelayError(shutdownServer(), closeConnections(), shutdownLoopResources())
        .then();
  }

  private Mono<Void> closeConnections() {
    return Mono.defer(
        () ->
            Mono.whenDelayError(
                    connections.stream()
                        .map(
                            connection -> {
                              connection.dispose();
                              return connection
                                  .onTerminate()
                                  .doOnError(e -> LOGGER.warn("Failed to close connection: " + e));
                            })
                        .collect(Collectors.toList()))
                .doOnTerminate(connections::clear));
  }

  private Mono<Void> shutdownLoopResources() {
    return Mono.defer(
        () ->
            Optional.ofNullable(loopResources)
                .map(
                    lr ->
                        lr.disposeLater()
                            .doOnError(
                                e ->
                                    LOGGER.warn(
                                        "Failed to close server transport loopResources: " + e)))
                .orElse(Mono.empty()));
  }

  private Mono<Void> shutdownServer() {
    return Mono.defer(
        () ->
            Optional.ofNullable(server)
                .map(
                    server -> {
                      server.dispose();
                      return server
                          .onClose()
                          .doOnError(e -> LOGGER.warn("Failed to close server: " + e));
                    })
                .orElse(Mono.empty()));
  }
}
