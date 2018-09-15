package io.scalecube.examples.orderbook;

import io.scalecube.examples.orderbook.service.DefaultMarketDataService;
import io.scalecube.examples.orderbook.service.OrderBookSnapshoot;
import io.scalecube.examples.orderbook.service.OrderRequest;
import io.scalecube.examples.orderbook.service.api.MarketDataService;
import io.scalecube.examples.orderbook.service.engine.Order;
import io.scalecube.examples.orderbook.service.engine.PriceLevel;
import io.scalecube.examples.orderbook.service.engine.events.Side;
import io.scalecube.services.Microservices;
import java.util.Collections;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Example1 {

  private static final String INSTRUMENT = "ORCL";

  private static final Random RANDOM = new Random(5);

  /**
   * Main method.
   *
   * @param args - program arguments
   * @throws InterruptedException - exception.
   */
  public static void main(String[] args) throws InterruptedException {

    Microservices gateway = Microservices.builder().startAwait();

    Microservices ms =
        Microservices.builder()
            .discovery(options -> options.seeds(gateway.discovery().address()))
            .services(new DefaultMarketDataService())
            .startAwait();

    MarketDataService marketService = ms.call().create().api(MarketDataService.class);

    marketService.orderBook().subscribe(Example1::print);

    Executors.newScheduledThreadPool(1)
        .scheduleAtFixedRate(
            () -> {
              try {

                if (RANDOM.nextInt(2) == 1) {
                  marketService
                      .processOrder(
                          new OrderRequest(
                              new Order(
                                  new PriceLevel(Side.BUY, RANDOM.nextInt(10) + 1), // prices
                                  System.currentTimeMillis(),
                                  Long.valueOf(RANDOM.nextInt(110) + 1 + "")), // units
                              INSTRUMENT))
                      .block();
                } else {
                  marketService
                      .processOrder(
                          new OrderRequest(
                              new Order(
                                  new PriceLevel(Side.SELL, RANDOM.nextInt(10) + 1), // prices
                                  System.currentTimeMillis(),
                                  Long.valueOf(RANDOM.nextInt(70) + 1 + "")), // units
                              INSTRUMENT))
                      .block();
                }
              } catch (Throwable ex) {
                ex.printStackTrace();
              }
            },
            3,
            3,
            TimeUnit.MILLISECONDS);

    Thread.currentThread().join();
  }

  private static void print(OrderBookSnapshoot snapshot) {

    System.out.println("====== Asks ========");
    System.out.println("  Price\t|  Amount");
    SortedMap<Long, Long> orderlist = new TreeMap<Long, Long>(Collections.reverseOrder());
    orderlist.putAll(snapshot.asks());
    orderlist.forEach((key, value) -> System.out.println("   " + key + "\t|    " + value));

    System.out.println("====================\nCurrent Price (" + snapshot.currentPrice() + ")");
    System.out.println("====== Bids ========");
    System.out.println("  Price\t|  Amount");
    snapshot.bids().forEach((key, value) -> System.out.println("   " + key + "\t|    " + value));
  }
}
