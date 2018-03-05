package io.scalecube.ipc;

import static io.scalecube.ipc.Event.Topic;

import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.function.Consumer;
import java.util.function.Function;

public class DefaultEventStream implements EventStream {

  private final Subject<Event, Event> subject = PublishSubject.<Event>create().toSerialized();
  private final Subject<Event, Event> closeSubject = PublishSubject.<Event>create().toSerialized();

  private final Function<Event, Event> eventMapper;

  public DefaultEventStream() {
    this(Function.identity());
  }

  public DefaultEventStream(Function<Event, Event> eventMapper) {
    this.eventMapper = eventMapper;
  }

  @Override
  public final void subscribe(ChannelContext channelContext) {
    channelContext.listen().subscribe(this::onNext, error -> onChannelContextInactiveDueError(channelContext, error));
    channelContext.listenClose(this::onChannelContextClosed);
  }

  @Override
  public final Observable<Event> listen() {
    return subject.onBackpressureBuffer().asObservable().map(eventMapper::apply);
  }

  @Override
  public final void close() {
    subject.onCompleted();
    closeSubject.onCompleted();
  }

  @Override
  public final void listenClose(Consumer<Void> onClose) {
    closeSubject.subscribe(event -> {
    }, throwable -> onClose.accept(null), () -> onClose.accept(null));
  }

  private void onNext(Event event) {
    subject.onNext(event);
  }

  private void onChannelContextClosed(ChannelContext channelContext) {
    subject.onNext(new Event.Builder(Topic.ChannelContextInactive, channelContext).build());
  }

  private void onChannelContextInactiveDueError(ChannelContext channelContext, Throwable throwable) {
    subject.onNext(new Event.Builder(Topic.ChannelContextInactive, channelContext).error(throwable).build());
  }
}
