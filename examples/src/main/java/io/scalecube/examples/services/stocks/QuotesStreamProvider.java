package io.scalecube.examples.services.stocks;

import rx.subjects.Subject;

public interface QuotesStreamProvider {

  Subject<Quote, Quote> subject();

}
