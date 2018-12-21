package io.scalecube.services.gateway;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import reactor.core.publisher.Mono;
import reactor.netty.FutureMono;
import reactor.netty.resources.LoopResources;

public class GatewayLoopResources implements LoopResources {

  private static final int BOSS_THREADS_NUM = 1;

  private static final DefaultThreadFactory BOSS_THREAD_FACTORY =
      new DefaultThreadFactory("boss-gateway", true);

  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;

  /**
   * Constructor for loop resources.
   *
   * @param workerGroup worker event loop group
   */
  public GatewayLoopResources(EventLoopGroup workerGroup) {
    this.workerGroup = workerGroup;
    this.bossGroup =
        Epoll.isAvailable()
            ? new EpollEventLoopGroup(BOSS_THREADS_NUM, BOSS_THREAD_FACTORY)
            : new NioEventLoopGroup(BOSS_THREADS_NUM, BOSS_THREAD_FACTORY);
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
    return Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class;
  }

  @Override
  public Class<? extends DatagramChannel> onDatagramChannel(EventLoopGroup group) {
    return Epoll.isAvailable() ? EpollDatagramChannel.class : NioDatagramChannel.class;
  }

  @Override
  public Class<? extends ServerChannel> onServerChannel(EventLoopGroup group) {
    return Epoll.isAvailable() ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
  }

  @Override
  public boolean preferNative() {
    return Epoll.isAvailable();
  }

  @Override
  public boolean daemon() {
    return true;
  }

  @Override
  public boolean isDisposed() {
    return bossGroup.isShutdown();
  }

  @Override
  public Mono<Void> disposeLater() {
    return Mono.defer(
        () -> {
          bossGroup.shutdownGracefully();
          //noinspection unchecked
          return FutureMono.from(((Future<Void>) bossGroup.terminationFuture()));
        });
  }
}
