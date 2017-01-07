package io.scalecube.examples.services;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import java.util.concurrent.CompletableFuture;

@Service
public interface TicketService {

  @ServiceMethod
  CompletableFuture<Boolean> reserve(int ticketCount);
}
