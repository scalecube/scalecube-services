package io.scalecube.examples.services;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import java.util.concurrent.CompletableFuture;

@Service
public interface UserService {

  @ServiceMethod
  CompletableFuture<Boolean> reserveTickets(Integer ticketsCount);

}
