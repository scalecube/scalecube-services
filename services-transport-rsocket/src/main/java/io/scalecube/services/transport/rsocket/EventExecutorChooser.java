package io.scalecube.services.transport.rsocket;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.EventExecutor;
import java.util.Iterator;

/**
 * Netty event executor chooser interface. Takes channel which is about to register and gets an
 * event executor for it. Channel will be bound to this executor later at {@link
 * io.netty.channel.EventLoopGroup#register(Channel)}.
 */
public interface EventExecutorChooser {

  EventExecutorChooser DEFAULT_INSTANCE =
      (channel, iterator) -> {
        while (iterator.hasNext()) {
          EventLoop eventLoop = (EventLoop) iterator.next();
          if (eventLoop.inEventLoop()) {
            return eventLoop;
          }
        }
        return null;
      };

  /**
   * Gets an event executor for unregistered channel.
   *
   * @param channel channel about to register
   * @param iterator available event loops
   * @return chosen event loop for the cahnnel
   */
  EventExecutor getEventExecutor(Channel channel, Iterator<EventExecutor> iterator);
}
