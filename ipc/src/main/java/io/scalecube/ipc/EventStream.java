package io.scalecube.ipc;

import rx.Observable;

import java.util.function.Consumer;

public interface EventStream {

  void subscribe(ChannelContext channelContext);

  Observable<Event> listen();

  void close();

  void listenClose(Consumer<Void> onClose);

  default Observable<Event> listenChannelContextInactive() {
    return listen().filter(Event::isChannelContextInactive);
  }

  default Observable<ServiceMessage> listenMessageReadSuccess() {
    return listen().filter(Event::isReadSuccess).map(Event::getMessageOrThrow);
  }

  default Observable<ServiceMessage> listenMessageWriteError() {
    return listen().filter(Event::isWriteError).map(Event::getMessageOrThrow);
  }

  default Observable<ServiceMessage> listenMessageWriteSuccess() {
    return listen().filter(Event::isWriteSuccess).map(Event::getMessageOrThrow);
  }
}
