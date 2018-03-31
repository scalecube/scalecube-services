package io.scalecube.examples.services.stocks;

import io.scalecube.services.Microservices;

public class StockMain {

  /**
   * Example that shows the basic usage of streaming quotes from a remove microservice. the example creates 2 cluster
   * nodes that communicates over the network: 1. gateway node cluster member that acts as seed and gateway. 2. service
   * nodes that provisions the SimpleQuoteService at the cluster.
   * 
   * <pre>
   *  
   * {@code
   *   Request: 
   *      QuoteService -> gateway (--- network --->) node1 -> SimpleQuoteService
   * 
   *   Stream: 
   *         onNext(quote) <- gateway (<--- network ---) node1 <- onNext(quote)
   * }
   * </pre>
   * the SimpleQuoteService generates random stock price every 1 second. each time subscriber subscribe he will get all
   * the history plus the next coming update of stock price.
   * the example for simplicity using both nodes on the same jvm for ease of debugging.
   * @param args none.
   */
  public static void main(String[] args) {

    // seed node as gateway
    Microservices gateway = Microservices.builder()
        .build();

    // cluster member node with SimpleQuoteService
    // join gateway cluster.
    Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService())
        .build();

    // create stock Quote service from gateway.
    QuoteService qouteService = gateway.call()
        .api(QuoteService.class); // Quote service interface.


    // subscribe on quotes changes
    qouteService.quotes("ORCL").subscribe(onNext -> {
      System.out.println(onNext);
    });

    
  }

}
