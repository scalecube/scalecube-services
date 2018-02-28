package io.scalecube.ipc;

import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.function.Function;

public class DefaultEventStream implements EventStream {

  private final Subject<Event, Event> subject = PublishSubject.<Event>create().toSerialized();

  private final Function<Event, Event> eventMapper;

  public DefaultEventStream() {
    this(Function.identity());
  }

  public DefaultEventStream(Function<Event, Event> eventMapper) {
    this.eventMapper = eventMapper;
  }

  @Override
  public final void subscribe(ChannelContext channelContext) {
    // Hint: at this point when we get onError/onCompleted on the channelContext we can forward those events (not
    // neccessarly as-is but as new Event types) to subject and hence give business layer ability to react on system
    // level events
    channelContext.listen().subscribe(subject::onNext,
        throwable -> {
        }, () -> {
        });
  }

  @Override
  public final Observable<Event> listen() {
    return subject.onBackpressureBuffer().asObservable().map(eventMapper::apply);
  }

  @Override
  public final void close() {
    subject.onCompleted();
  }
}
