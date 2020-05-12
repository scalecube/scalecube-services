package io.scalecube.services.transport.rsocket;

import io.rsocket.core.RSocketServer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.scalecube.net.Address;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.tcp.TcpServer;

/** RSocket server transport implementation. */
public class RSocketServerTransport implements ServerTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServerTransport.class);

  private final ServiceMessageCodec codec;
  private final TcpServer tcpServer;

  private CloseableChannel serverChannel; // calculated

  /**
   * Constructor for this server transport.
   *
   * @param codec message codec
   * @param tcpServer tcp server
   */
  public RSocketServerTransport(ServiceMessageCodec codec, TcpServer tcpServer) {
    this.codec = codec;
    this.tcpServer = tcpServer;
  }

  @Override
  public Address address() {
    InetSocketAddress address = serverChannel.address();
    return Address.create(address.getHostString(), address.getPort());
  }

  @Override
  public Mono<ServerTransport> bind(
      ServiceMethodRegistry methodRegistry, Authenticator authenticator) {
    return Mono.defer(
        () -> {
          TcpServer tcpServer =
              this.tcpServer.doOnConnection(
                  connection -> {
                    LOGGER.info("Accepted connection on {}", connection.channel());
                    connection.onDispose(
                        () -> LOGGER.info("Connection closed on {}", connection.channel()));
                  });
          return RSocketServer.create()
              .acceptor(new RSocketServiceAcceptor(codec, methodRegistry, authenticator))
              .payloadDecoder(PayloadDecoder.DEFAULT)
              .bind(TcpServerTransport.create(tcpServer))
              .doOnSuccess(channel -> serverChannel = channel)
              .thenReturn(this);
        });
  }

  @Override
  public Mono<Void> stop() {
    return Mono.defer(
        () -> {
          if (serverChannel == null) {
            return Mono.empty();
          }
          serverChannel.dispose();
          return serverChannel
              .onClose()
              .doOnError(e -> LOGGER.warn("Failed to close server: " + e));
        });
  }

  @Override
  public String toString() {
    return "RSocketServerTransport{"
        + "codec="
        + codec
        + ", tcpServer="
        + tcpServer
        + ", serverChannel="
        + serverChannel
        + '}';
  }
}
