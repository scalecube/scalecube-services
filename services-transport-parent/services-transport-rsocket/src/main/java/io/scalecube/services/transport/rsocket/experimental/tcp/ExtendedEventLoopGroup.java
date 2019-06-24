package io.scalecube.services.transport.rsocket.experimental.tcp;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.EventExecutor;
import java.util.Iterator;

/** Mix-in for custom event loop group. */
public interface ExtendedEventLoopGroup {

  /**
   * Select event loop from multithreading loop group.
   *
   * @param channel channel
   * @param executors executors
   * @return event loop
   */
  default EventLoop chooseEventLoop(Channel channel, Iterator<EventExecutor> executors) {
    while (executors.hasNext()) {
      EventExecutor eventLoop = executors.next();
      if (eventLoop.inEventLoop()) {
        return (EventLoop) eventLoop;
      }
    }
    return null;
  }
}
