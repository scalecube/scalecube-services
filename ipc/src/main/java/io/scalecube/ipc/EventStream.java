package io.scalecube.ipc;

import rx.Observable;

import java.util.function.Consumer;

public interface EventStream {

  void subscribe(ChannelContext channelContext);

  Observable<Event> listen();

  void close();

  void listenClose(Consumer<Void> onClose);

  default Observable<ServiceMessage> listenMessageReadSuccess() {
    return listen().filter(Event::isReadSuccess).map(Event::getMessageOrThrow);
  }

  default Observable<ServiceMessage> listenMessageWrite() {
    return listen().filter(Event::isMessageWrite).map(Event::getMessageOrThrow);
  }
}
