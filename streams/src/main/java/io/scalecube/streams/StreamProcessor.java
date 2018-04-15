package io.scalecube.streams;

import rx.Observable;
import rx.Observer;

public interface StreamProcessor<U, V> extends Observer<U> {

  Observable<V> listen();

  void close();
}
