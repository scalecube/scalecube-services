package io.scalecube.services.transport.rsocket.experimental;

import io.rsocket.RSocketFactory;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.scalecube.net.Address;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import io.scalecube.services.transport.api.experimental.ServerTransport;
import io.scalecube.services.transport.rsocket.RSocketServerTransport;
import io.scalecube.services.transport.rsocket.RSocketServiceAcceptor;
import io.scalecube.services.transport.rsocket.experimental.RSocketServerTransportFactory.Server;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class RSocketScalecubeServerTransport implements ServerTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServerTransport.class);

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
  public Mono<ServerTransport> bind(Address address, ServiceMethodRegistry methodRegistry) {
    return Mono.defer(
        () -> RSocketFactory.receive()
            .frameDecoder(PayloadDecoder.ZERO_COPY)
            .errorConsumer(
                th -> LOGGER.warn("Exception occurred at rsocket server transport: " + th))
            .acceptor(new RSocketServiceAcceptor(codec, methodRegistry))
            .transport(transportFactory.createServer(address))
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
