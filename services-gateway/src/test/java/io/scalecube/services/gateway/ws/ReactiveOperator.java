package io.scalecube.services.gateway.ws;

import reactor.core.Disposable;

public interface ReactiveOperator extends Disposable {

  void dispose(Throwable throwable);

  void lastError(Throwable throwable);

  Throwable lastError();

  void tryNext(Object fragment);

  boolean isFastPath();

  void commitProduced();

  long incrementProduced();

  long requested(long limit);
}
