package io.scalecube.ipc;

import rx.Observable;

public interface EventStream {

  void subscribe(ChannelContext channelContext);

  Observable<Event> listen();

  default Observable<Event> listenReadSuccess() {
    return listen().filter(Event::isReadSuccess);
  }

  default Observable<Event> listenReadError() {
    return listen().filter(Event::isReadError);
  }

  default Observable<Event> listenWriteSuccess() {
    return listen().filter(Event::isWriteSuccess);
  }

  default Observable<Event> listenWriteError() {
    return listen().filter(Event::isWriteError);
  }

  void close();
}
