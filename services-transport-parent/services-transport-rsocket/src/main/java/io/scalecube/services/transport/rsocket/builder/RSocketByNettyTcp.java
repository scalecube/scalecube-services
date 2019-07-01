package io.scalecube.services.transport.rsocket.builder;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import io.netty.bootstrap.BootstrapConfig;
import io.netty.bootstrap.ServerBootstrapConfig;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import io.scalecube.services.transport.api.ServiceTransportProvider;
import io.scalecube.services.transport.rsocket.RSocketScalecubeClientTransport;
import io.scalecube.services.transport.rsocket.RSocketScalecubeServerTransport;
import io.scalecube.services.transport.rsocket.RSocketServiceTransportProvider;
import io.scalecube.services.transport.rsocket.tcp.NettyTcpTransportFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.netty.ReactorNetty;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;

/** Builder for RSocket Transport based on Netty Tcp. */
public class RSocketByNettyTcp {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketByNettyTcp.class);

  private TcpClient tcpClient;
  private TcpServer tcpServer;
  private HeadersCodec headersCodec;
  private Collection<DataCodec> dataCodecs = new ArrayList<>();
  private int workerCount = Integer.parseInt(System.getProperty(
      ReactorNetty.IO_WORKER_COUNT,
      "" + Math.max(Runtime.getRuntime()
          .availableProcessors(), 4)));

  private RSocketByNettyTcp() {
    this.tcpClient = TcpClient.newConnection();
    this.tcpServer = TcpServer.create();
  }

  /**
   * Create builder.
   *
   * @return builder
   */
  public static RSocketByNettyTcp builder() {
    return new RSocketByNettyTcp();
  }

  /**
   * Let customize {@link TcpClient}. <br>
   * Example: <br>
   * {@code builder.customizeClient(client -> client.runOn(eventLoopGroup));}
   *
   * @param customizer customizer function
   * @return current builder
   */
  public RSocketByNettyTcp customizeClient(Function<TcpClient, TcpClient> customizer) {
    this.tcpClient = requireNonNull(customizer, "Must be not null").apply(this.tcpClient);
    return this;
  }

  /**
   * Setting worker count for Tcp Resources.
   *
   * @param numberOfWorker number of worker
   * @return current builder
   */
  public RSocketByNettyTcp workerCount(int numberOfWorker) {
    this.workerCount = numberOfWorker;
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
  public RSocketByNettyTcp customizeServer(Function<TcpServer, TcpServer> customizer) {
    this.tcpServer = requireNonNull(customizer, "Must be not null").apply(this.tcpServer);
    return this;
  }

  /**
   * Set headers codec.
   *
   * @param headersCodec headers codec
   * @return current builder
   */
  public RSocketByNettyTcp headersCodec(HeadersCodec headersCodec) {
    this.headersCodec = headersCodec;
    return this;
  }

  /**
   * Set DataCodecs for encode-decode message's body.
   *
   * @param dataCodecs data codecs
   * @return current builder
   */
  public RSocketByNettyTcp dataCodecs(Collection<DataCodec> dataCodecs) {
    this.dataCodecs.addAll(dataCodecs);
    return this;
  }

  /**
   * Set DataCodecs for encode-decode message's body.
   *
   * @param dataCodecs data codecs
   * @return current builder
   */
  public RSocketByNettyTcp dataCodecs(DataCodec... dataCodecs) {
    return dataCodecs(asList(dataCodecs));
  }

  /**
   * Build of ServiceTransportProvider.
   *
   * @return transport for scalecube
   */
  public ServiceTransportProvider build() {
    setResourcesIfNotExists();
    NettyTcpTransportFactory transportFactory = new NettyTcpTransportFactory(tcpClient, tcpServer);
    ServiceMessageCodec codec = new ServiceMessageCodec(headersCodec, dataCodecs);
    RSocketScalecubeServerTransport serverTransport =
        new RSocketScalecubeServerTransport(transportFactory, codec);
    RSocketScalecubeClientTransport clientTransport =
        new RSocketScalecubeClientTransport(transportFactory, codec);
    return new RSocketServiceTransportProvider(clientTransport, serverTransport);
  }

  private void setResourcesIfNotExists() {
    BootstrapConfig clientConfig = tcpClient.configure().config();
    ServerBootstrapConfig serverConfig = tcpServer.configure().config();
    List<Disposable> disposables = new ArrayList<>(2);
    if (clientConfig.group() == null) {
      LoopResources resources = LoopResources.create("scalecube-client", workerCount, true);
      this.tcpClient = tcpClient.runOn(resources, true);
    }
    if (serverConfig.group() == null) {
      LoopResources resources = LoopResources.create("scalecube-server", 1, workerCount, true);
      this.tcpServer = tcpServer.runOn(resources, true);
      disposables.add(resources);
    }
    registerShutdownHook(disposables);
  }

  private void registerShutdownHook(Collection<Disposable> resources) {
    if (resources == null || resources.size() == 0) {
      return;
    }
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  for (Disposable disposable : resources) {
                    try {
                      if (!disposable.isDisposed()) {
                        disposable.dispose();
                      }
                    } catch (Exception e) {
                      LOGGER.error(e.getMessage(), e);
                    }
                  }
                }));
  }
}
