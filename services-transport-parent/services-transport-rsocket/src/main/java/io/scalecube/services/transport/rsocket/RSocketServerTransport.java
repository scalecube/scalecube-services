package io.scalecube.services.transport.rsocket;

import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.scalecube.services.Address;
import io.scalecube.services.auth.Authenticator;
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
  private final int mtu;
  private final int maxMessageSize;

  private CloseableChannel serverChannel; // calculated

  /**
   * Constructor for this server transport. Fragmentation is disabled and message size is unbounded.
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
    this(authenticator, serviceRegistry, headersCodec, dataCodecs, serverTransportFactory, 0, 0);
  }

  /**
   * Constructor for this server transport.
   *
   * @param authenticator authenticator
   * @param serviceRegistry serviceRegistry
   * @param headersCodec headersCodec
   * @param dataCodecs dataCodecs
   * @param serverTransportFactory serverTransportFactory
   * @param mtu fragmentation MTU in bytes; {@code 0} disables fragmentation (see {@link
   *     io.rsocket.core.RSocketServer#fragment(int)})
   * @param maxMessageSize maximum message size in bytes; {@code 0} means unbounded. Bounds both the
   *     encoded response size (responses larger than this fail fast with {@link
   *     MessageTooLargeException} instead of being framed) and inbound reassembly (see {@link
   *     io.rsocket.core.RSocketServer#maxInboundPayloadSize(int)}), preventing OOM in either
   *     direction.
   */
  public RSocketServerTransport(
      Authenticator authenticator,
      ServiceRegistry serviceRegistry,
      HeadersCodec headersCodec,
      Collection<DataCodec> dataCodecs,
      RSocketServerTransportFactory serverTransportFactory,
      int mtu,
      int maxMessageSize) {
    this.authenticator = authenticator;
    this.serviceRegistry = serviceRegistry;
    this.headersCodec = headersCodec;
    this.dataCodecs = dataCodecs;
    this.serverTransportFactory = serverTransportFactory;
    this.mtu = mtu;
    this.maxMessageSize = maxMessageSize;
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
      final var rsocketServer =
          RSocketServer.create()
              .acceptor(
                  new RSocketServiceAcceptor(
                      headersCodec, dataCodecs, authenticator, serviceRegistry, maxMessageSize));
      if (mtu > 0) {
        rsocketServer.fragment(mtu);
      }
      if (maxMessageSize >= RSocketConstants.MAX_FRAME_LENGTH) {
        rsocketServer.maxInboundPayloadSize(maxMessageSize);
      }
      serverChannel =
          rsocketServer.bind(serverTransportFactory.serverTransport()).toFuture().get();
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
