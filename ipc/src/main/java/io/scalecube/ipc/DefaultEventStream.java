package io.scalecube.ipc;

import static io.scalecube.ipc.Event.Topic;

import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;
import rx.subscriptions.CompositeSubscription;

import java.util.function.Consumer;
import java.util.function.Function;

public class DefaultEventStream implements EventStream {

  private final Subject<Event, Event> subject = PublishSubject.<Event>create().toSerialized();
  private final Subject<Event, Event> closeSubject = PublishSubject.<Event>create().toSerialized();

  private final CompositeSubscription subscriptions = new CompositeSubscription();

  private final Function<Event, Event> eventMapper;

  public DefaultEventStream() {
    this(Function.identity());
  }

  public DefaultEventStream(Function<Event, Event> eventMapper) {
    this.eventMapper = eventMapper;
  }

  @Override
  public final void subscribe(ChannelContext channelContext) {
    // register cleanup process upfront
    channelContext.listenClose(this::onChannelContextClosed);
    // bind channelContext to this event stream
    subscriptions.add(channelContext.listen().subscribe(this::onNext));
  }

  @Override
  public final Observable<Event> listen() {
    return subject.onBackpressureBuffer().asObservable().map(eventMapper::apply);
  }

  @Override
  public final void close() {
    subject.onCompleted();
    closeSubject.onCompleted();
    subscriptions.clear();
  }

  @Override
  public final void listenClose(Consumer<Void> onClose) {
    closeSubject.subscribe(event -> {
    }, throwable -> onClose.accept(null), () -> onClose.accept(null));
  }

  private void onNext(Event event) {
    subject.onNext(event);
  }

  private void onChannelContextClosed(ChannelContext ctx) {
    subject.onNext(new Event.Builder(Topic.ChannelContextClosed, ctx.getAddress(), ctx.getId()).build());
  }
}
