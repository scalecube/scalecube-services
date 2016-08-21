package io.scalecube.transport;

import rx.Observable;
import rx.Scheduler;

import java.util.concurrent.Executor;

public interface IListenable<T> {

  Observable<T> listen();

  Observable<T> listen(Executor executor);

  Observable<T> listen(Scheduler scheduler);
}
