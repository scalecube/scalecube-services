package io.scalecube.services.transport.rsocket.experimental.builder;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import io.scalecube.services.transport.api.experimental.ServiceTransportProvider;
import io.scalecube.services.transport.rsocket.experimental.RSocketScalecubeClientTransport;
import io.scalecube.services.transport.rsocket.experimental.RSocketScalecubeServerTransport;
import io.scalecube.services.transport.rsocket.experimental.RSocketServiceTransportProvider;
import io.scalecube.services.transport.rsocket.experimental.tcp.NettyTcpTransportFactory;
import io.scalecube.services.transport.rsocket.experimental.tcp.TcpLoopResources;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;

/** Builder for RSocket Transport based on Netty Tcp. */
public class RSocketNettyTcp {

  private TcpClient tcpClient;
  private TcpServer tcpServer;
  private HeadersCodec headersCodec;
  private Collection<DataCodec> dataCodecs = new ArrayList<>();

  private RSocketNettyTcp() {
    LoopResources clientResources = TcpLoopResources.clientLoopResources();
    this.tcpClient = TcpClient.newConnection().runOn(clientResources);
    LoopResources serverResources = TcpLoopResources.serverLoopResources();
    this.tcpServer = TcpServer.create().runOn(serverResources);
  }

  /**
   * Create builder.
   *
   * @return builder
   */
  public static RSocketNettyTcp builder() {
    return new RSocketNettyTcp();
  }

  /**
   * Let customize {@link TcpClient}. <br>
   * Example: <br>
   * {@code builder.customizeClient(client -> client.runOn(eventLoopGroup));}
   *
   * @param customizer customizer function
   * @return current builder
   */
  public RSocketNettyTcp customizeClient(Function<TcpClient, TcpClient> customizer) {
    this.tcpClient = requireNonNull(customizer, "Must be not null").apply(this.tcpClient);
    return this;
  }

  /**
   * Let customize {@link TcpServer}. <br>
   * Example: <br>
   * {@code builder.customizeClient(server -> server.runOn(eventLoopGroup));}
   *
   * @param customizer customizer function
   * @return current builder
   */
  public RSocketNettyTcp customizeServer(Function<TcpServer, TcpServer> customizer) {
    this.tcpServer = requireNonNull(customizer, "Must be not null").apply(this.tcpServer);
    return this;
  }

  /**
   * Set headers codec.
   *
   * @param headersCodec headers codec
   * @return current builder
   */
  public RSocketNettyTcp headersCodec(HeadersCodec headersCodec) {
    this.headersCodec = headersCodec;
    return this;
  }

  /**
   * Set DataCodecs for encode-decode message's body.
   *
   * @param dataCodecs data codecs
   * @return current builder
   */
  public RSocketNettyTcp dataCodecs(Collection<DataCodec> dataCodecs) {
    this.dataCodecs.addAll(dataCodecs);
    return this;
  }

  /**
   * Set DataCodecs for encode-decode message's body.
   *
   * @param dataCodecs data codecs
   * @return current builder
   */
  public RSocketNettyTcp dataCodecs(DataCodec... dataCodecs) {
    return dataCodecs(asList(dataCodecs));
  }

  /**
   * Build of ServiceTransportProvider.
   *
   * @return transport for scalecube
   */
  public ServiceTransportProvider build() {
    NettyTcpTransportFactory transportFactory = new NettyTcpTransportFactory(tcpClient, tcpServer);
    ServiceMessageCodec codec = new ServiceMessageCodec(headersCodec, dataCodecs);
    RSocketScalecubeServerTransport serverTransport =
        new RSocketScalecubeServerTransport(transportFactory, codec);
    RSocketScalecubeClientTransport clientTransport =
        new RSocketScalecubeClientTransport(transportFactory, codec);
    return new RSocketServiceTransportProvider(clientTransport, serverTransport);
  }
}
