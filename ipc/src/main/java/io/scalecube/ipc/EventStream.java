package io.scalecube.ipc;

import rx.Observable;

import java.util.function.Consumer;

public interface EventStream {

  void subscribe(Observable<Event> observable, Consumer<Throwable> onError, Consumer<Void> onCompleted);

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
