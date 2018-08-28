package io.scalecube.services.transport.rsocket;

import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.scalecube.services.transport.api.WorkerThreadChooser;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

/**
 * Default event excutor chooser which delegates job on selection to the provided {@link
 * WorkerThreadChooser} instance.
 */
public class DefaultEventExecutorChooser implements EventExecutorChooser {

  private final WorkerThreadChooser threadChooser;

  public DefaultEventExecutorChooser(WorkerThreadChooser threadChooser) {
    this.threadChooser = threadChooser;
  }

  @Override
  public EventExecutor getEventExecutor(Channel channel, Iterator<EventExecutor> iterator) {
    EventExecutor[] executors =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
            .toArray(EventExecutor[]::new);

    for (EventExecutor executor : executors) {
      if (executor.inEventLoop()) {
        return executor;
      }
    }

    String channelId = channel.id().asLongText(); // globally unique id
    SocketAddress localAddress = channel.localAddress(); // bound address
    SocketAddress remoteAddress = channel.remoteAddress(); // remote ephemeral address

    try {
      return (EventExecutor)
          threadChooser.getWorker(channelId, localAddress, remoteAddress, executors);
    } catch (Exception ex) {
      return null; // this is just fine
    }
  }
}
