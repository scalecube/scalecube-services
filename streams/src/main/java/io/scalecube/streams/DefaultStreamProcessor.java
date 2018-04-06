package io.scalecube.streams;

import io.scalecube.streams.exceptions.StreamExceptionMapper;

import rx.Emitter;
import rx.Observable;
import rx.Observer;
import rx.subscriptions.CompositeSubscription;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DefaultStreamProcessor implements StreamProcessor {

  public static final StreamMessage onCompletedMessage =
      StreamMessage.builder().qualifier(Qualifier.Q_ON_COMPLETED).build();

  private final ChannelContext channelContext;
  private final EventStream eventStream;
  private final StreamExceptionMapper exceptionMapper;

  private final AtomicBoolean isTerminated = new AtomicBoolean(); // state

  /**
   * Constructor for this stream processor.
   * 
   * @param channelContext channel context
   * @param eventStream event stream
   * @param exceptionMapper exception mapper
   */
  public DefaultStreamProcessor(ChannelContext channelContext, EventStream eventStream,
      StreamExceptionMapper exceptionMapper) {
    this.channelContext = channelContext;
    this.eventStream = eventStream;
    this.exceptionMapper = exceptionMapper;
    // bind channel context to event stream
    this.eventStream.subscribe(this.channelContext);
  }

  @Override
  public void onCompleted() {
    if (isTerminated.compareAndSet(false, true)) {
      channelContext.postWrite(onCompletedMessage);
    }
  }

  @Override
  public void onError(Throwable throwable) {
    if (isTerminated.compareAndSet(false, true)) {
      channelContext.postWrite(exceptionMapper.toMessage(throwable));
    }
  }

  @Override
  public void onNext(StreamMessage message) {
    if (!isTerminated.get()) {
      channelContext.postWrite(message);
    }
  }

  @Override
  public Observable<StreamMessage> listen() {
    return Observable.create(emitter -> {

      CompositeSubscription subscriptions = new CompositeSubscription();
      emitter.setCancellation(subscriptions::clear);

      // message logic: remote read => onMessage
      subscriptions.add(
          channelContext.listenReadSuccess()
              .map(Event::getMessageOrThrow)
              .subscribe(message -> onMessage(message, emitter)));

      // error logic: failed remote write => observer error
      subscriptions.add(
          channelContext.listenWriteError()
              .map(Event::getErrorOrThrow)
              .subscribe(emitter::onError));

      // connection logic: connection lost => observer error
      subscriptions.add(
          eventStream.listenChannelContextClosed()
              .filter(event -> event.getAddress().equals(channelContext.getAddress()))
              .map(event -> new IOException("ChannelContext closed on address: " + event.getAddress()))
              .subscribe(emitter::onError));

    }, Emitter.BackpressureMode.BUFFER);
  }

  @Override
  public void close() {
    // this alone will unsubscribe this channel context
    // from local stream => no more requests, no more replies
    channelContext.close();
  }

  private void onMessage(StreamMessage message, Observer<StreamMessage> emitter) {
    Qualifier qualifier = Qualifier.fromString(message.qualifier());
    if (Qualifier.Q_ON_COMPLETED.asString().equalsIgnoreCase(qualifier.asString())) { // remote => onCompleted
      emitter.onCompleted();
      return;
    }
    if (Qualifier.Q_ERROR_NAMESPACE.equalsIgnoreCase(qualifier.getNamespace())) { // remote => onError
      emitter.onError(exceptionMapper.toException(qualifier, message.data()));
      return;
    }
    emitter.onNext(message); // remote => normal response
  }
}
