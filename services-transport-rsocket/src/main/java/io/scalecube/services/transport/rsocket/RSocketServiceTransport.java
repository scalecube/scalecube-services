package io.scalecube.services.transport.rsocket;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.internal.PlatformDependent;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import io.scalecube.services.transport.api.ServiceTransport;
import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.function.BiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.FutureMono;

/**
 * RSocket service transport. Entry point for getting {@link RSocketClientTransport} and {@link
 * RSocketServerTransport}.
 */
public class RSocketServiceTransport implements ServiceTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServiceTransport.class);

  private static final ThreadFactory WORKER_THREAD_FACTORY =
      new DefaultThreadFactory("rsocket-worker", true);

  private static final String DEFAULT_HEADERS_FORMAT = "application/json";
  private static final String EPOLL_CLASS_NAME = "io.netty.channel.epoll.Epoll";
  private static boolean preferEpoll = false;

  static {
    if (PlatformDependent.isWindows()) {
      LOGGER.warn("Epoll is not supported by this environment, NIO will be used");
    } else {
      try {
        Class.forName(EPOLL_CLASS_NAME);
        preferEpoll = Epoll.isAvailable();
      } catch (ClassNotFoundException e) {
        LOGGER.warn("Cannot load Epoll, NIO will be used", e);
      }
    }
    LOGGER.debug("Epoll support: " + preferEpoll);
  }

  private final BiFunction<Channel, Iterator<EventExecutor>, EventExecutor> eventLoopChooserFn =
      (channel, iterator) -> {
        while (iterator.hasNext()) {
          EventLoop eventLoop = (EventLoop) iterator.next();
          if (eventLoop.inEventLoop()) {
            return eventLoop;
          }
        }
        return null;
      };

  @Override
  public boolean isNativeSupported() {
    return preferEpoll;
  }

  @Override
  public ClientTransport getClientTransport(Executor workerThreadPool) {
    return new RSocketClientTransport(
        new ServiceMessageCodec(HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT)),
        new DelegatedLoopResources(preferEpoll, (EventLoopGroup) workerThreadPool));
  }

  @Override
  public ServerTransport getServerTransport(Executor workerThreadPool) {
    return new RSocketServerTransport(
        new ServiceMessageCodec(HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT)),
        preferEpoll,
        (EventLoopGroup) workerThreadPool);
  }

  @Override
  public Executor getWorkerThreadPool(int numOfThreads) {
    return preferEpoll
        ? new ExtendedEpollEventLoopGroup(numOfThreads, WORKER_THREAD_FACTORY, eventLoopChooserFn)
        : new ExtendedNioEventLoopGroup(numOfThreads, WORKER_THREAD_FACTORY, eventLoopChooserFn);
  }

  @Override
  public Mono<Void> shutdown(Executor workerThreadPool) {
    //noinspection unchecked
    return Mono.defer(
        () ->
            workerThreadPool != null
                ? FutureMono.from((Future) ((EventLoopGroup) workerThreadPool).shutdownGracefully())
                : Mono.empty());
  }
}
