package io.scalecube.services.gateway.websocket;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Mono;

@Service(HeartbeatService.NAMESPACE)
public interface HeartbeatService {

  String NAMESPACE = "v1/scalecube.websocket.heartbeat";

  @ServiceMethod
  Mono<Long> ping(long value);
}
