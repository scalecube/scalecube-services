package io.scalecube.services.streaming;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import rx.Observable;

@Service(QuoteService.NAME)
public interface QuoteService {

  String NAME = "io.sc.quote-service";

  @ServiceMethod
  Observable<String> quotes(int maxSize);

  @ServiceMethod
  Observable<String> snapshoot(int size);

  @ServiceMethod
  Observable<String> justOne();

  @ServiceMethod
  Observable<String> scheduled(int interval);
  
}
