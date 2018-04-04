package io.scalecube.streams;

import rx.Observable;
import rx.Observer;

public interface StreamProcessor<ObservableType, ObserverType> extends Observer<ObserverType> {

  Observable<ObservableType> listen();

  void close();
}
