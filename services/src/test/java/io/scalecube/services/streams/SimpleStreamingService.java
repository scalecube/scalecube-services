package io.scalecube.services.streams;

import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleStreamingService implements StreamingService {

  private final Subject<String, String> quotes = PublishSubject.<String>create().toSerialized();
  
  public SimpleStreamingService() {
    
    final AtomicInteger i = new AtomicInteger(0);
    
    ScheduledExecutorService job = Executors.newScheduledThreadPool(1);
    
    job.scheduleAtFixedRate(()->{
      quotes.onNext(i.get()+"");
      i.incrementAndGet();
    }, 1, 1, TimeUnit.SECONDS);
    
  }

  @Override
  public Observable<String> quotes(int maxSize) {
    return quotes.asObservable();
  }

}
