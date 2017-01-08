package io.scalecube.examples.services;

import io.scalecube.services.annotations.Inject;

import java.util.concurrent.CompletableFuture;

public class UserServiceImpl implements UserService {

  private TicketService ticketService;

  @Inject
  private UserServiceConfig userConfig;

  @Inject
  public UserServiceImpl(TicketService ticketService) {
    this.ticketService = ticketService;
  }

  @Override
  public CompletableFuture<Boolean> reserveTickets(Integer ticketsCount) {
    if(ticketsCount > userConfig.getMaxTicketAllowed())
      throw new RuntimeException("invalid ticket count");
    
    return ticketService.reserve(ticketsCount);
  }

  public static final class UserServiceConfig {

    private int maxTicketAllowed;

    public UserServiceConfig(int maxTicketAllowed) {
      this.maxTicketAllowed = maxTicketAllowed;
    }

    public int getMaxTicketAllowed() {
      return maxTicketAllowed;
    }

  }
}
