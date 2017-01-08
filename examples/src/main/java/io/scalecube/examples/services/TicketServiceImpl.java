package io.scalecube.examples.services;

import io.scalecube.services.annotations.Inject;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class TicketServiceImpl implements TicketService {

  AtomicInteger maxTicketCount = new AtomicInteger(10);

  @Inject
  public TicketServiceImpl(TickerServiceConfig config) {
    this.maxTicketCount = new AtomicInteger(config.getMaxTicketCount());
  }

  @Override
  public CompletableFuture<Boolean> reserve(int ticketCount) {
    return CompletableFuture.supplyAsync(() -> maxTicketCount.addAndGet(-ticketCount) >= 0);
  }

  public static final class TickerServiceConfig {

    int maxTicketCount;

    public TickerServiceConfig(int maxTicketCount) {
      this.maxTicketCount = maxTicketCount;
    }

    public int getMaxTicketCount() {
      return maxTicketCount;
    }

  }
}
