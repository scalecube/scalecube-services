package io.scalecube.services.examples.orderbook;

import io.scalecube.net.Address;
import io.scalecube.services.Scalecube;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.examples.orderbook.service.DefaultMarketDataService;
import io.scalecube.services.examples.orderbook.service.OrderBookSnapshoot;
import io.scalecube.services.examples.orderbook.service.OrderRequest;
import io.scalecube.services.examples.orderbook.service.api.MarketDataService;
import io.scalecube.services.examples.orderbook.service.engine.Order;
import io.scalecube.services.examples.orderbook.service.engine.PriceLevel;
import io.scalecube.services.examples.orderbook.service.engine.events.Side;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
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

    Scalecube gateway =
        Scalecube.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(RSocketServiceTransport::new)
            .startAwait();

    final Address gatewayAddress = gateway.discovery().address();

    Scalecube ms =
        Scalecube.builder()
            .discovery(
                endpoint ->
                    new ScalecubeServiceDiscovery(endpoint)
                        .membership(cfg -> cfg.seedMembers(gatewayAddress)))
            .transport(RSocketServiceTransport::new)
            .services(new DefaultMarketDataService())
            .startAwait();

    MarketDataService marketService = ms.call().api(MarketDataService.class);

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
                                  Long.parseLong(RANDOM.nextInt(110) + 1 + "")), // units
                              INSTRUMENT))
                      .block();
                } else {
                  marketService
                      .processOrder(
                          new OrderRequest(
                              new Order(
                                  new PriceLevel(Side.SELL, RANDOM.nextInt(10) + 1), // prices
                                  System.currentTimeMillis(),
                                  Long.parseLong(RANDOM.nextInt(70) + 1 + "")), // units
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
    SortedMap<Long, Long> orderlist = new TreeMap<>(Collections.reverseOrder());
    orderlist.putAll(snapshot.asks());
    orderlist.forEach((key, value) -> System.out.println("   " + key + "\t|    " + value));

    System.out.println("====================\nCurrent Price (" + snapshot.currentPrice() + ")");
    System.out.println("====== Bids ========");
    System.out.println("  Price\t|  Amount");
    snapshot.bids().forEach((key, value) -> System.out.println("   " + key + "\t|    " + value));
  }
}
