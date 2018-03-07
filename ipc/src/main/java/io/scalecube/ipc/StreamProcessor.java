package io.scalecube.ipc;

import rx.Observable;
import rx.Observer;

public interface StreamProcessor extends Observer<ServiceMessage> {

  ServiceMessage onErrorMessage =
      ServiceMessage.withQualifier(Qualifier.Q_GENERAL_FAILURE).build();

  ServiceMessage onCompletedMessage =
      ServiceMessage.withQualifier(Qualifier.Q_ON_COMPLETED).build();

  Observable<ServiceMessage> listen();

  void close();
}
