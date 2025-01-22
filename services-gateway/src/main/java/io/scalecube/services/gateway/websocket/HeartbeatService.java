package io.scalecube.services.gateway.websocket;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Mono;

/**
 * Service interface for handling custom ping/pong service messages for websocket - service is
 * echoing back ping message to the client. Used (optionally) as part of {@link WebsocketGateway}.
 */
@Service(HeartbeatService.NAMESPACE)
public interface HeartbeatService {

  String NAMESPACE = "v1/scalecube.websocket";

  @ServiceMethod
  Mono<Long> ping(long value);
}
