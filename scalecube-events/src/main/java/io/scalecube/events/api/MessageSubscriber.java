package io.scalecube.events.api;

import reactor.core.publisher.Flux;

public interface MessageSubscriber {

  Flux<String> listen();
  
}

