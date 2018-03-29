package io.scalecube.streams;

import io.scalecube.transport.Address;

import rx.Observable;

import java.util.function.Consumer;

public interface EventStream {

  void subscribe(ChannelContext channelContext);

  Observable<Event> listen();

  void onNext(Event event);

  void onNext(Address address, Event event);

  void close();

  void listenClose(Consumer<Void> onClose);

  default Observable<Event> listenReadSuccess() {
    return listen().filter(Event::isReadSuccess);
  }

  default Observable<Event> listenReadError() {
    return listen().filter(Event::isReadError);
  }

  default Observable<Event> listenWrite() {
    return listen().filter(Event::isWrite);
  }

  default Observable<Event> listenWriteSuccess() {
    return listen().filter(Event::isWriteSuccess);
  }

  default Observable<Event> listenWriteError() {
    return listen().filter(Event::isWriteError);
  }

  default Observable<Event> listenChannelContextClosed() {
    return listen().filter(Event::isChannelContextClosed);
  }

  default Observable<Event> listenChannelContextSubscribed() {
    return listen().filter(Event::isChannelContextSubscribed);
  }

  default Observable<Event> listenChannelContextUnsubscribed() {
    return listen().filter(Event::isChannelContextUnsubscribed);
  }
}
