package io.scalecube.services.streams;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import rx.Observable;

@Service(QuoteService.NAME)
public interface QuoteService {

  String NAME = "sc-quote-service";

  @ServiceMethod
  Observable<String> quotes(int index);

  @ServiceMethod
  Observable<String> snapshoot(int index);
}
