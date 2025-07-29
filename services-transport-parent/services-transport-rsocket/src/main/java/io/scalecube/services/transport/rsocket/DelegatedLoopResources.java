package io.scalecube.services.transport.rsocket;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import reactor.core.publisher.Mono;
import reactor.netty.FutureMono;
import reactor.netty.resources.LoopResources;

/**
 * Loop resources implementation based on already constructed boss event loop group and worker event
 * loop group. This loop resources instance doesn't allocate any resources by itself.
 */
public class DelegatedLoopResources implements LoopResources {

  private static final int BOSS_THREADS_NUM = 1;

  private static final DefaultThreadFactory BOSS_THREAD_FACTORY =
      new DefaultThreadFactory("boss-transport", true);

  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private final AtomicBoolean running = new AtomicBoolean(true);

  private DelegatedLoopResources(EventLoopGroup bossGroup, EventLoopGroup workerGroup) {
    this.bossGroup = bossGroup;
    this.workerGroup = workerGroup;
  }

  /**
   * Creates loop resources for client side.
   *
   * @param workerGroup worker pool
   * @return loop resources
   */
  public static DelegatedLoopResources newClientLoopResources(EventLoopGroup workerGroup) {
    return new DelegatedLoopResources(null /*bossGroup*/, workerGroup);
  }

  /**
   * Creates new loop resources for server side.
   *
   * @param workerGroup worker pool
   * @return loop resources
   */
  public static DelegatedLoopResources newServerLoopResources(EventLoopGroup workerGroup) {
    EventLoopGroup bossGroup = new NioEventLoopGroup(BOSS_THREADS_NUM, BOSS_THREAD_FACTORY);
    return new DelegatedLoopResources(bossGroup, workerGroup);
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
  public boolean daemon() {
    return true;
  }

  @Override
  public boolean isDisposed() {
    return !running.get();
  }

  @Override
  public Mono<Void> disposeLater() {
    return Mono.defer(
        () -> {
          Mono<Void> promise = Mono.empty();
          if (running.compareAndSet(true, false)) {
            if (bossGroup != null) {
              //noinspection unchecked
              promise = FutureMono.from((Future) bossGroup.shutdownGracefully());
            }
          }
          return promise;
        });
  }

  @Override
  public String toString() {
    return "DelegatedLoopResources{"
        + "bossGroup="
        + bossGroup
        + ", workerGroup="
        + workerGroup
        + ", running="
        + running
        + '}';
  }
}
