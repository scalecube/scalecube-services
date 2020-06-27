package io.scalecube.services.transport.rsocket;

import io.rsocket.core.RSocketServer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.scalecube.net.Address;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import java.net.InetSocketAddress;
import java.util.StringJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class RSocketServerTransport implements ServerTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServerTransport.class);

  private final ServiceMessageCodec messageCodec;
  private final RSocketServerTransportFactory serverTransportFactory;

  private CloseableChannel serverChannel; // calculated

  /**
   * Constructor for this server transport.
   *
   * @param messageCodec messageCodec
   * @param serverTransportFactory serverTransportFactory
   */
  public RSocketServerTransport(
      ServiceMessageCodec messageCodec, RSocketServerTransportFactory serverTransportFactory) {
    this.messageCodec = messageCodec;
    this.serverTransportFactory = serverTransportFactory;
  }

  @Override
  public Address address() {
    InetSocketAddress socketAddress = serverChannel.address();
    return Address.create(socketAddress.getAddress().getHostAddress(), socketAddress.getPort());
  }

  @Override
  public Mono<ServerTransport> bind(ServiceMethodRegistry methodRegistry) {
    return Mono.defer(
        () ->
            RSocketServer.create()
                .acceptor(new RSocketServiceAcceptor(messageCodec, methodRegistry))
                .payloadDecoder(PayloadDecoder.DEFAULT)
                .bind(serverTransportFactory.serverTransport())
                .doOnSuccess(channel -> serverChannel = channel)
                .thenReturn(this));
  }

  @Override
  public Mono<Void> stop() {
    return Mono.defer(
        () -> {
          if (serverChannel == null || serverChannel.isDisposed()) {
            return Mono.empty();
          }
          return Mono.fromRunnable(() -> serverChannel.dispose())
              .then(
                  serverChannel
                      .onClose()
                      .doOnError(
                          e ->
                              LOGGER.warn(
                                  "[rsocket][server][onClose] Exception occurred: {}",
                                  e.toString())));
        });
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", RSocketServerTransport.class.getSimpleName() + "[", "]")
        .add("messageCodec=" + messageCodec)
        .add("serverChannel=" + serverChannel)
        .toString();
  }
}
