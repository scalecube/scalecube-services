package io.scalecube.services.transport.rsocket;

import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.scalecube.services.Address;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.ServerTransport;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RSocketServerTransport implements ServerTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServerTransport.class);

  private final Authenticator authenticator;
  private final ServiceRegistry serviceRegistry;
  private final HeadersCodec headersCodec;
  private final Collection<DataCodec> dataCodecs;
  private final RSocketServerTransportFactory serverTransportFactory;

  private CloseableChannel serverChannel; // calculated

  /**
   * Constructor for this server transport.
   *
   * @param authenticator authenticator
   * @param serviceRegistry serviceRegistry
   * @param headersCodec headersCodec
   * @param dataCodecs dataCodecs
   * @param serverTransportFactory serverTransportFactory
   */
  public RSocketServerTransport(
      Authenticator authenticator,
      ServiceRegistry serviceRegistry,
      HeadersCodec headersCodec,
      Collection<DataCodec> dataCodecs,
      RSocketServerTransportFactory serverTransportFactory) {
    this.authenticator = authenticator;
    this.serviceRegistry = serviceRegistry;
    this.headersCodec = headersCodec;
    this.dataCodecs = dataCodecs;
    this.serverTransportFactory = serverTransportFactory;
  }

  @Override
  public Address address() {
    final String host = serverChannel.address().getAddress().getHostAddress();
    final int port = serverChannel.address().getPort();
    return Address.create(host, port);
  }

  @Override
  public ServerTransport bind() {
    try {
      RSocketServer.create()
          .acceptor(
              new RSocketServiceAcceptor(headersCodec, dataCodecs, authenticator, serviceRegistry))
          .bind(serverTransportFactory.serverTransport())
          .doOnSuccess(channel -> serverChannel = channel)
          .toFuture()
          .get();
      return this;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {
    if (serverChannel != null && !serverChannel.isDisposed()) {
      try {
        serverChannel.dispose();
        serverChannel.onClose().toFuture().get();
      } catch (Exception e) {
        LOGGER.warn("[serverChannel][onClose] Exception: {}", e.toString());
      }
    }
  }
}
