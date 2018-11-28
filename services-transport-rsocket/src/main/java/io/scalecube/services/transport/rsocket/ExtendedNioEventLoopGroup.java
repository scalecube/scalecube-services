package io.scalecube.services.transport.rsocket;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.SelectStrategy;
import io.netty.channel.SelectStrategyFactory;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.RejectedExecutionHandler;
import java.lang.reflect.Constructor;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.function.BiFunction;

/**
 * Nio event loop group with provided event executor chooser. Creates instances of {@link
 * io.netty.channel.nio.NioEventLoop}.
 */
public class ExtendedNioEventLoopGroup extends NioEventLoopGroup {

  private final BiFunction<Channel, Iterator<EventExecutor>, EventLoop> eventLoopChooser;

  /**
   * Constructor for event loop.
   *
   * @param numOfThreads number of worker threads
   * @param threadFactory thread factory
   * @param eventLoopChooser executor chooser
   */
  public ExtendedNioEventLoopGroup(
      int numOfThreads,
      ThreadFactory threadFactory,
      BiFunction<Channel, Iterator<EventExecutor>, EventLoop> eventLoopChooser) {
    super(numOfThreads, threadFactory);
    this.eventLoopChooser = eventLoopChooser;
  }

  @Override
  protected EventLoop newChild(Executor executor, Object... args) throws Exception {
    Class<?> eventLoopClass = Class.forName("io.netty.channel.nio.NioEventLoop");

    Constructor<?> constructor =
        eventLoopClass.getDeclaredConstructor(
            NioEventLoopGroup.class,
            Executor.class,
            SelectorProvider.class,
            SelectStrategy.class,
            RejectedExecutionHandler.class);

    constructor.setAccessible(true);

    return (EventLoop)
        constructor.newInstance(
            this,
            executor,
            (SelectorProvider) args[0],
            ((SelectStrategyFactory) args[1]).newSelectStrategy(),
            (RejectedExecutionHandler) args[2]);
  }

  @Override
  public ChannelFuture register(Channel channel) {
    EventLoop eventExecutor = eventLoopChooser.apply(channel, iterator());
    return eventExecutor != null
        ? eventExecutor.register(channel)
        : super.register(channel);
  }

  @Override
  public ChannelFuture register(ChannelPromise promise) {
    return register(promise.channel());
  }
}
