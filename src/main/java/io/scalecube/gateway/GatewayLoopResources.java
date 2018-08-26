package io.scalecube.gateway;

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
import java.util.concurrent.atomic.AtomicBoolean;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.resources.LoopResources;

public class GatewayLoopResources implements LoopResources {

  private final boolean preferEpoll;
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private final AtomicBoolean running = new AtomicBoolean(true);

  /**
   * Constructor for loop resources.
   *
   * @param preferEpoll should use epoll or nio
   * @param bossGroup selector event loop group
   * @param workerGroup worker event loop group
   */
  public GatewayLoopResources(
    boolean preferEpoll, EventLoopGroup bossGroup, EventLoopGroup workerGroup) {
    this.preferEpoll = preferEpoll;
    this.bossGroup = bossGroup;
    this.workerGroup = workerGroup;
  }

  @Override
  public EventLoopGroup onClient(boolean useNative) {
    return workerGroup;
  }

  @Override
  public EventLoopGroup onServer(boolean useNative) {
    return workerGroup;
  }

  @Override
  public EventLoopGroup onServerSelect(boolean useNative) {
    return bossGroup;
  }

  @Override
  public Class<? extends Channel> onChannel(EventLoopGroup group) {
    return preferEpoll ? EpollSocketChannel.class : NioSocketChannel.class;
  }

  @Override
  public Class<? extends DatagramChannel> onDatagramChannel(EventLoopGroup group) {
    return preferEpoll ? EpollDatagramChannel.class : NioDatagramChannel.class;
  }

  @Override
  public Class<? extends ServerChannel> onServerChannel(EventLoopGroup group) {
    return preferEpoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
  }

  @Override
  public boolean preferNative() {
    return preferEpoll;
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
