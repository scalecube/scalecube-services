package io.scalecube.services.streams;

import rx.Observable;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleQuoteService implements QuoteService {

  private final Subject<String, String> quotes = PublishSubject.<String>create();
  final AtomicInteger i = new AtomicInteger(1);

  final ScheduledExecutorService job = Executors.newScheduledThreadPool(1);

  public SimpleQuoteService() {}

  @Override
  public Observable<String> quotes(int maxSize) {
    job.scheduleAtFixedRate(() -> {
      if ((i.get() % maxSize) == 0) {
        quotes.onNext("quote : " + i.get());
      }
      i.incrementAndGet();
    }, 1, 1, TimeUnit.MILLISECONDS);

    return quotes.serialize();
  }

  @Override
  public Observable<String> snapshoot(int size) {
    CompletableFuture.runAsync(() -> {
      for (int i = 0; i < size; i++) {
        pause(i); // slow down sending.
        quotes.onNext("quote : " + i);
      }
    });

    return quotes.onBackpressureBuffer();
  }

  private void pause(int i) {
    if (i % 5000 == 0) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
      }
    }
  }

}
