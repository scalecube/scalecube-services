package io.scalecube.services.transport.rsocket.tcp;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultSelectStrategyFactory;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.SelectStrategy;
import io.netty.channel.SelectStrategyFactory;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import java.lang.reflect.Constructor;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

public class ExtendedEpollEventLoopGroup extends MultithreadEventLoopGroup
    implements ExtendedEventLoopGroup {

  private static final ThreadFactory WORKER_THREAD_FACTORY =
      new DefaultThreadFactory("rsocket-worker", true);

  /**
   * Constructor for event loop.
   *
   * @param numOfThreads number of worker threads
   */
  public ExtendedEpollEventLoopGroup(int numOfThreads) {
    super(
        numOfThreads,
        WORKER_THREAD_FACTORY,
        0,
        DefaultSelectStrategyFactory.INSTANCE,
        RejectedExecutionHandlers.reject());
  }

  @Override
  protected EventLoop newChild(Executor executor, Object... args) throws Exception {
    Class<?> eventLoopClass = Class.forName("io.netty.channel.epoll.EpollEventLoop");

    Constructor<?> constructor =
        eventLoopClass.getDeclaredConstructor(
            EventLoopGroup.class,
            Executor.class,
            int.class,
            SelectStrategy.class,
            RejectedExecutionHandler.class);

    constructor.setAccessible(true);

    return (EventLoop)
        constructor.newInstance(
            this,
            executor,
            (Integer) args[0],
            ((SelectStrategyFactory) args[1]).newSelectStrategy(),
            (RejectedExecutionHandler) args[2]);
  }

  @Override
  public ChannelFuture register(Channel channel) {
    EventLoop eventExecutor = chooseEventLoop(channel, iterator());
    return eventExecutor != null ? eventExecutor.register(channel) : super.register(channel);
  }

  @Override
  public ChannelFuture register(ChannelPromise promise) {
    return register(promise.channel());
  }
}
