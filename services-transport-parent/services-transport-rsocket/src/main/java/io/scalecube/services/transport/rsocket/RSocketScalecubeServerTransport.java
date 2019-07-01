package io.scalecube.services.transport.rsocket;

import io.rsocket.RSocketFactory;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.net.Address;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import io.scalecube.services.transport.rsocket.RSocketServerTransportFactory.Server;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class RSocketScalecubeServerTransport implements ServerTransport {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(RSocketScalecubeServerTransport.class);

  private final RSocketServerTransportFactory transportFactory;
  private final ServiceMessageCodec codec;

  private Server server; // calculated

  public RSocketScalecubeServerTransport(
      RSocketServerTransportFactory transportFactory, ServiceMessageCodec codec) {
    this.transportFactory = transportFactory;
    this.codec = codec;
  }

  @Override
  public Address address() {
    return server.address();
  }

  @Override
  public Mono<ServerTransport> bind(ServiceMethodRegistry methodRegistry) {
    return Mono.defer(
        () ->
            RSocketFactory.receive()
                .frameDecoder(
                    frame ->
                        ByteBufPayload.create(
                            frame.sliceData().retain(), frame.sliceMetadata().retain()))
                .errorConsumer(
                    th -> LOGGER.warn("Exception occurred at rsocket server transport: " + th))
                .acceptor(new RSocketServiceAcceptor(codec, methodRegistry))
                .transport(transportFactory.createServerTransport())
                .start()
                .doOnSuccess(this::setServer)
                .thenReturn(this));
  }

  @Override
  public Mono<Void> stop() {
    return Mono.defer(
        () ->
            this.getServer()
                .map(
                    server -> {
                      server.dispose();
                      return server
                          .onClose()
                          .doOnError(e -> LOGGER.warn("Failed to close server: " + e));
                    })
                .orElse(Mono.empty()));
  }

  protected Optional<Server> getServer() {
    return Optional.ofNullable(server);
  }

  protected void setServer(Server server) {
    this.server = server;
  }
}
