package io.scalecube.services.transport.rsocket;

import io.rsocket.core.RSocketServer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.scalecube.net.Address;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import java.net.InetSocketAddress;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class RSocketServerTransport implements ServerTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServerTransport.class);

  private final ServiceMethodRegistry methodRegistry;
  private final ConnectionSetupCodec connectionSetupCodec;
  private final HeadersCodec headersCodec;
  private final Collection<DataCodec> dataCodecs;
  private final RSocketServerTransportFactory serverTransportFactory;

  private CloseableChannel serverChannel; // calculated

  /**
   * Constructor for this server transport.
   *
   * @param methodRegistry methodRegistry
   * @param connectionSetupCodec connectionSetupCodec
   * @param headersCodec headersCodec
   * @param dataCodecs dataCodecs
   * @param serverTransportFactory serverTransportFactory
   */
  public RSocketServerTransport(
      ServiceMethodRegistry methodRegistry,
      ConnectionSetupCodec connectionSetupCodec,
      HeadersCodec headersCodec,
      Collection<DataCodec> dataCodecs,
      RSocketServerTransportFactory serverTransportFactory) {
    this.methodRegistry = methodRegistry;
    this.connectionSetupCodec = connectionSetupCodec;
    this.headersCodec = headersCodec;
    this.dataCodecs = dataCodecs;
    this.serverTransportFactory = serverTransportFactory;
  }

  @Override
  public Address address() {
    InetSocketAddress socketAddress = serverChannel.address();
    return Address.create(socketAddress.getAddress().getHostAddress(), socketAddress.getPort());
  }

  @Override
  public Mono<ServerTransport> bind() {
    return Mono.defer(
        () ->
            RSocketServer.create()
                .acceptor(
                    new RSocketServiceAcceptor(
                        connectionSetupCodec,
                        new ServiceMessageCodec(headersCodec, dataCodecs),
                        methodRegistry))
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
}
