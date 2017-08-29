package io.scalecube.examples.services.stocks;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import rx.Observable;

@Service(QuoteService.NAME)
public interface QuoteService {

  String NAME = "io.sc.quote-service";

  @ServiceMethod
  Observable<Quote> quotes(String symbol);

}
