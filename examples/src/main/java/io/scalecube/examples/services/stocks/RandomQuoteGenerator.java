package io.scalecube.examples.services.stocks;

import rx.subjects.ReplaySubject;
import rx.subjects.Subject;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RandomQuoteGenerator implements QuotesStreamProvider {

  private final ScheduledExecutorService quoutGenerator = Executors.newScheduledThreadPool(1);
  private final Subject<Quote, Quote> quotes = ReplaySubject.<Quote>create();

  private final ConcurrentMap<String, Quote> qoutesData = new ConcurrentHashMap<>();

  public RandomQuoteGenerator() {
    qoutesData.putIfAbsent("AXP", Quote.create("NASDC", "AXP", 17.5F));
    qoutesData.putIfAbsent("AAPL", Quote.create("NASDC", "AAPL", 18.3F));
    qoutesData.putIfAbsent("ORCL", Quote.create("NYCE", "ORCL", 13.1F));
    start();
  }

  @Override
  public Subject<Quote, Quote> subject() {
    return quotes;
  }

  private void start() {
    quoutGenerator.scheduleWithFixedDelay(() -> {
      qoutesData.forEach((symbol, quote) -> {
        quotes.onNext(quote);
        double price = generateRandom();
        qoutesData.put(symbol, quote.update(price));
      });

    }, 1, 1, TimeUnit.SECONDS);
  }

  private double generateRandom() {
    return ((Math.random() * 100) / 1000) + 1;
  }
}
