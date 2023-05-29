package io.scalecube.services.transport.rsocket;

import io.rsocket.core.RSocketServer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.scalecube.net.Address;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.ServerTransport;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RSocketServerTransport implements ServerTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServerTransport.class);

  private final Authenticator<Object> authenticator;
  private final ServiceMethodRegistry methodRegistry;
  private final ConnectionSetupCodec connectionSetupCodec;
  private final HeadersCodec headersCodec;
  private final Collection<DataCodec> dataCodecs;
  private final RSocketServerTransportFactory serverTransportFactory;

  private CloseableChannel serverChannel; // calculated

  /**
   * Constructor for this server transport.
   *
   * @param authenticator authenticator
   * @param methodRegistry methodRegistry
   * @param connectionSetupCodec connectionSetupCodec
   * @param headersCodec headersCodec
   * @param dataCodecs dataCodecs
   * @param serverTransportFactory serverTransportFactory
   */
  public RSocketServerTransport(
      Authenticator<Object> authenticator,
      ServiceMethodRegistry methodRegistry,
      ConnectionSetupCodec connectionSetupCodec,
      HeadersCodec headersCodec,
      Collection<DataCodec> dataCodecs,
      RSocketServerTransportFactory serverTransportFactory) {
    this.authenticator = authenticator;
    this.methodRegistry = methodRegistry;
    this.connectionSetupCodec = connectionSetupCodec;
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
              new RSocketServiceAcceptor(
                  connectionSetupCodec, headersCodec, dataCodecs, authenticator, methodRegistry))
          .payloadDecoder(PayloadDecoder.DEFAULT)
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
    if (serverChannel == null || serverChannel.isDisposed()) {
      return;
    }

    try {
      serverChannel.dispose();
      serverChannel.onClose().toFuture().get();
    } catch (Exception e) {
      LOGGER.warn("[serverChannel][onClose] Exception: {}", e.toString());
    }
  }
}
