package io.scalecube.ipc;

import rx.Observable;
import rx.Observer;

public interface StreamProcessor extends Observer<ServiceMessage> {

  Observable<ServiceMessage> listen();

  void close();
}
