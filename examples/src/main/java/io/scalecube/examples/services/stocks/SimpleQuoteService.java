package io.scalecube.examples.services.stocks;

import rx.Observable;
import rx.subjects.Subject;

public class SimpleQuoteService implements QuoteService {

  private QuotesStreamProvider source = new RandomQuoteGenerator();

  @Override
  public Observable<Quote> quotes(String symbol) {
    Subject<Quote, Quote> quotes = source.subject();
    return quotes.onBackpressureBuffer().filter(onNext -> onNext.ticker().equals(symbol));
  }

}
