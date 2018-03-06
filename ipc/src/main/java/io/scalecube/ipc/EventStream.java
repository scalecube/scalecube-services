package io.scalecube.ipc;

import io.scalecube.transport.Address;

import rx.Observable;

import java.util.function.Consumer;

public interface EventStream {

  void subscribe(ChannelContext channelContext);

  void unsubscribe(Address address);

  Observable<Event> listen();

  void close();

  void listenClose(Consumer<Void> onClose);

  default Observable<Event> listenChannelContextClosed() {
    return listen().filter(Event::isChannelContextClosed);
  }

  default Observable<Event> listenChannelContextUnsubscribed() {
    return listen().filter(Event::isChannelContextUnsubscribed);
  }

  default Observable<ServiceMessage> listenMessageReadSuccess() {
    return listen().filter(Event::isReadSuccess).map(Event::getMessageOrThrow);
  }

  default Observable<ServiceMessage> listenMessageWriteSuccess() {
    return listen().filter(Event::isWriteSuccess).map(Event::getMessageOrThrow);
  }
}
