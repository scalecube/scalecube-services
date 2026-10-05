package io.scalecube.services.gateway.websocket;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Flux;

@Service
public interface HangingService {

  @ServiceMethod
  Flux<String> hang(String request);
}
