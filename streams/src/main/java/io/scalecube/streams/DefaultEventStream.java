package io.scalecube.streams;

import io.scalecube.transport.Address;

import rx.Observable;
import rx.Subscription;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

public class DefaultEventStream implements EventStream {

  private final Subject<Event, Event> subject = PublishSubject.<Event>create();
  private final Subject<Event, Event> closeSubject = PublishSubject.<Event>create();

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
    AtomicBoolean valueComputed = new AtomicBoolean();
    subscriptions.computeIfAbsent(channelContext, channelContext1 -> {
      Subscription subscription = subscribe0(channelContext1);
      valueComputed.set(true);
      return subscription;
    });
    if (valueComputed.get()) { // computed in lambda
      channelContext.listenClose(this::onChannelContextClosed);
      onChannelContextSubscribed(channelContext);
    }
  }

  @Override
  public final Observable<Event> listen() {
    return subject.map(eventMapper::apply);
  }

  @Override
  public void onNext(Event event) {
    subject.onNext(event);
  }

  @Override
  public void onNext(Address address, Event event) {
    Stream<ChannelContext> channelContextStream = subscriptions.keySet().stream();
    channelContextStream.filter(ctx -> ctx.getAddress().equals(address)).forEach(ctx -> ctx.onNext(event));
  }

  @Override
  public final void close() {
    // cleanup subscriptions
    for (ChannelContext channelContext : subscriptions.keySet()) {
      Subscription subscription = subscriptions.remove(channelContext);
      if (subscription != null) {
        subscription.unsubscribe();
      }
    }
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

  private Subscription subscribe0(ChannelContext channelContext) {
    return channelContext.listen()
        .doOnUnsubscribe(() -> onChannelContextUnsubscribed(channelContext))
        .subscribe(subject::onNext);
  }

  private void onChannelContextClosed(ChannelContext ctx) {
    subject.onNext(Event.channelContextClosed(ctx.getAddress()).identity(ctx.getId()).build());
  }

  private void onChannelContextSubscribed(ChannelContext ctx) {
    subject.onNext(Event.channelContextSubscribed(ctx.getAddress()).identity(ctx.getId()).build());
  }

  private void onChannelContextUnsubscribed(ChannelContext ctx) {
    subject.onNext(Event.channelContextUnsubscribed(ctx.getAddress()).identity(ctx.getId()).build());
  }
}
