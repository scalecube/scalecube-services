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
    return ticketService.reserve(ticketsCount);
  }

  public static final class UserServiceConfig {

    private String ticketVenue;

    public UserServiceConfig(String ticketVenue) {
      this.ticketVenue = ticketVenue;
    }

    public String getTicketVenue() {
      return ticketVenue;
    }

  }
}
