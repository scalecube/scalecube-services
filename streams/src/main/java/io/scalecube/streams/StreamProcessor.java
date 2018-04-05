package io.scalecube.streams;

import rx.Observable;
import rx.Observer;

public interface StreamProcessor<REQ, RESP> extends Observer<REQ> {

  Observable<RESP> listen();

  void close();
}
