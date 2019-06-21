package io.scalecube.services.transport.rsocket.experimental.builder;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.util.concurrent.EventExecutor;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import io.scalecube.services.transport.rsocket.DelegatedLoopResources;
import io.scalecube.services.transport.rsocket.ExtendedEpollEventLoopGroup;
import io.scalecube.services.transport.rsocket.ExtendedNioEventLoopGroup;
import io.scalecube.services.transport.rsocket.experimental.RSocketScalecubeClientTransport;
import io.scalecube.services.transport.rsocket.experimental.RSocketScalecubeServerTransport;
import io.scalecube.services.transport.rsocket.experimental.RSocketServiceTransportProvider;
import io.scalecube.services.transport.rsocket.experimental.tcp.NettyTcpTransportFactory;
import java.util.Collection;
import java.util.Iterator;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;

public class RSocketNettyTcp {

  private TcpClient tcpClient;
  private TcpServer tcpServer;
  private HeadersCodec headersCodec;
  private Collection<DataCodec> dataCodecs;
  private int numOfWorkers = Runtime.getRuntime().availableProcessors();

  public static RSocketNettyTcp create() {
    return new RSocketNettyTcp();
  }

  private RSocketNettyTcp() {
    EventLoopGroup workerGroup = eventLoopGroup();
    LoopResources clientResources = DelegatedLoopResources.newClientLoopResources(workerGroup);
    this.tcpClient = TcpClient.newConnection().runOn(clientResources);
    LoopResources serverResources = DelegatedLoopResources.newServerLoopResources(workerGroup);
    this.tcpServer = TcpServer.create().runOn(serverResources);
  }

  public RSocketServiceTransportProvider build() {
    NettyTcpTransportFactory transportFactory = new NettyTcpTransportFactory(tcpClient,
        tcpServer);
    ServiceMessageCodec codec = new ServiceMessageCodec(headersCodec, dataCodecs);
    RSocketScalecubeServerTransport serverTransport = new RSocketScalecubeServerTransport(
        transportFactory, codec);
    RSocketScalecubeClientTransport clientTransport = new RSocketScalecubeClientTransport(
        transportFactory, codec);
    return new RSocketServiceTransportProvider(clientTransport, serverTransport);
  }

  private EventLoopGroup eventLoopGroup() {
    return Epoll.isAvailable()
        ? new ExtendedEpollEventLoopGroup(numOfWorkers, this::chooseEventLoop)
        : new ExtendedNioEventLoopGroup(numOfWorkers, this::chooseEventLoop);
  }

  private EventLoop chooseEventLoop(Channel channel, Iterator<EventExecutor> executors) {
    while (executors.hasNext()) {
      EventExecutor eventLoop = executors.next();
      if (eventLoop.inEventLoop()) {
        return (EventLoop) eventLoop;
      }
    }
    return null;
  }

}
