package io.scalecube.services.transport.rsocket.experimental.tcp;

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
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Mono;
import reactor.netty.FutureMono;
import reactor.netty.resources.LoopResources;

/**
 * Loop resources implementation based on already constructed boss event loop group and worker event
 * loop group. This loop resources instance doesn't allocate any resources by itself.
 */
public class TcpLoopResources implements LoopResources {

  private static final int BOSS_THREADS_NUM = 1;

  private static final DefaultThreadFactory BOSS_THREAD_FACTORY =
      new DefaultThreadFactory("boss-transport", true);

  private static final AtomicReference<LoopResources> resources = new AtomicReference<>();
  private final EventLoopGroup workerGroup;
  private EventLoopGroup bossGroup;

  private TcpLoopResources(EventLoopGroup bossGroup) {
    this.workerGroup = workerGroup();
    this.bossGroup = bossGroup == null ? this.workerGroup : bossGroup;
  }

  private TcpLoopResources() {
    this(null);
  }

  /**
   * Creates loop resources for client side.
   *
   * @return loop resources
   */
  public static LoopResources clientLoopResources() {
    return resources.updateAndGet(res -> res == null ? new TcpLoopResources() : res);
  }

  /**
   * Creates new loop resources for server side.
   *
   * @return loop resources
   */
  public static LoopResources serverLoopResources() {
    return resources.updateAndGet(res -> res == null ? new TcpLoopResources(bossGroup()) : res);
  }

  private static EventLoopGroup bossGroup() {
    return Epoll.isAvailable()
        ? new EpollEventLoopGroup(BOSS_THREADS_NUM, BOSS_THREAD_FACTORY)
        : new NioEventLoopGroup(BOSS_THREADS_NUM, BOSS_THREAD_FACTORY);
  }

  private EventLoopGroup workerGroup() {
    return Epoll.isAvailable()
        ? new ExtendedEpollEventLoopGroup(DEFAULT_IO_WORKER_COUNT)
        : new ExtendedNioEventLoopGroup(DEFAULT_IO_WORKER_COUNT);
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
    return bossGroup != null && bossGroup.isShutdown() && workerGroup.isShutdown();
  }

  @Override
  public Mono<Void> disposeLater() {
    return Mono.defer(
        () -> {
          workerGroup.shutdownGracefully();
          Mono<Void> closedWorkerGroup =
              FutureMono.from(((Future<Void>) workerGroup.terminationFuture()));
          if (bossGroup == null) {
            return closedWorkerGroup;
          }
          bossGroup.shutdownGracefully();
          //noinspection unchecked
          return closedWorkerGroup.then(
              FutureMono.from(((Future<Void>) bossGroup.terminationFuture())));
        });
  }
}
