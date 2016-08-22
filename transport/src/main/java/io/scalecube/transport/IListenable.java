package io.scalecube.transport;

import rx.Observable;
import rx.Scheduler;

public interface IListenable<T> {

  Observable<T> listen();

  Observable<T> listen(Scheduler scheduler);
}
