package io.scalecube.streams;

import rx.Observable;
import rx.Observer;

public interface StreamProcessor extends Observer<StreamMessage> {

  Observable<StreamMessage> listen();

  void unsubscribe();
}
