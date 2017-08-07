package io.scalecube.services.streams;

import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleQuoteService implements QuoteService {

  private final Subject<String, String> quotes = PublishSubject.<String>create();
  final AtomicInteger i = new AtomicInteger(0);
  
  final ScheduledExecutorService job = Executors.newScheduledThreadPool(1);
  
  public SimpleQuoteService() {  
  }

  @Override
  public Observable<String> quotes(int maxSize) {
    job.scheduleAtFixedRate(()->{
      quotes.onNext("quote : " + i.get());
      i.incrementAndGet();
    }, 1, 1, TimeUnit.SECONDS);
    
    return quotes.serialize();
  }

}
