package io.scalecube.streams;

import rx.Observable;
import rx.Observer;

public interface StreamProcessor<ObserverType, ObservableType> extends Observer<ObserverType> {

  Observable<ObservableType> listen();

  void close();
}
