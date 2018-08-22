package io.scalecube.gateway.rsocket.websocket;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import java.util.concurrent.atomic.AtomicBoolean;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.resources.LoopResources;

public class RSocketWebsocketLoopResources implements LoopResources {

  private final EventLoopGroup bossEventLoopGroup;
  private final EventLoopGroup workerEventLoopGroup;
  private final AtomicBoolean running = new AtomicBoolean(true);

  public RSocketWebsocketLoopResources(
    EventLoopGroup bossEventLoopGroup, EventLoopGroup workerEventLoopGroup) {
    this.bossEventLoopGroup = bossEventLoopGroup;
    this.workerEventLoopGroup = workerEventLoopGroup;
  }

  @Override
  public Class<? extends Channel> onChannel(EventLoopGroup group) {
    return RSocketServiceTransport.isEpollSupported()
      ? EpollSocketChannel.class
      : NioSocketChannel.class;
  }

  @Override
  public EventLoopGroup onClient(boolean useNative) {
    return workerEventLoopGroup;
  }

  @Override
  public Class<? extends DatagramChannel> onDatagramChannel(EventLoopGroup group) {
    return RSocketServiceTransport.isEpollSupported()
      ? EpollDatagramChannel.class
      : NioDatagramChannel.class;
  }

  @Override
  public EventLoopGroup onServer(boolean useNative) {
    return workerEventLoopGroup;
  }

  @Override
  public Class<? extends ServerChannel> onServerChannel(EventLoopGroup group) {
    return RSocketServiceTransport.isEpollSupported()
      ? EpollServerSocketChannel.class
      : NioServerSocketChannel.class;
  }

  @Override
  public EventLoopGroup onServerSelect(boolean useNative) {
    return bossEventLoopGroup;
  }

  @Override
  public boolean preferNative() {
    return RSocketServiceTransport.isEpollSupported();
  }

  @Override
  public boolean daemon() {
    return true;
  }

  @Override
  public void dispose() {
    // no-op
  }

  @Override
  public boolean isDisposed() {
    return !running.get();
  }

  @Override
  public Mono<Void> disposeLater() {
    return Mono.defer(
      () -> {
        running.compareAndSet(true, false);
        return Mono.empty();
      });
  }
}
