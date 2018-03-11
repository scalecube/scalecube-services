package io.scalecube.ipc;

import static io.scalecube.ipc.Event.Topic;

import io.scalecube.transport.Address;

import rx.Observable;
import rx.Subscription;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;

public class DefaultEventStream implements EventStream {

  private final Subject<Event, Event> subject = PublishSubject.<Event>create().toSerialized();
  private final Subject<Event, Event> closeSubject = PublishSubject.<Event>create().toSerialized();

  private final ConcurrentMap<ChannelContext, Subscription> subscriptions = new ConcurrentHashMap<>();

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

    Subscription subscription = channelContext.listen()
        .doOnUnsubscribe(() -> onChannelContextUnsubscribed(channelContext))
        .subscribe(this::onNext);

    subscriptions.put(channelContext, subscription);

    onChannelContextSubscribed(channelContext);
  }

  @Override
  public final Observable<Event> listen() {
    return subject.onBackpressureBuffer().asObservable().map(eventMapper::apply);
  }

  @Override
  public void unsubscribe(Address address) {
    // fullscan and filter, then remove and unsubscribe
    subscriptions.keySet().stream()
        .filter(ctx -> ctx.getAddress().equals(address))
        .map(subscriptions::remove)
        .forEach(Subscription::unsubscribe);
  }

  @Override
  public final void close() {
    // cleanup subscriptions
    subscriptions.forEach((ctx, subscription) -> subscription.unsubscribe());
    subscriptions.clear();
    // complete subjects
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

  private void onChannelContextClosed(ChannelContext ctx) {
    subject.onNext(new Event.Builder(Topic.ChannelContextClosed, ctx.getAddress(), ctx.getId()).build());
  }

  private void onChannelContextSubscribed(ChannelContext ctx) {
    subject.onNext(new Event.Builder(Topic.ChannelContextSubscribed, ctx.getAddress(), ctx.getId()).build());
  }

  private void onChannelContextUnsubscribed(ChannelContext ctx) {
    subject.onNext(new Event.Builder(Topic.ChannelContextUnsubscribed, ctx.getAddress(), ctx.getId()).build());
  }
}
