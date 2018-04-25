package io.scalecube.services.transport.client.api;

import io.scalecube.services.api.ServiceMessage;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ClientChannel {

  /**
   * Request-Response interaction model of {@code RSocket}.
   *
   * @param request Request payload.
   * @return {@code Publisher} containing at most a single {@code ServiceMessage} representing the response.
   */
  public Mono<ServiceMessage> requestResponse(ServiceMessage request);

  /**
   * Request-Stream interaction model of.
   *
   * @param ServiceMessage Request.
   * @return {@code Publisher} containing the stream of {@code ServiceMessage}s representing the response.
   */
  public Flux<ServiceMessage> requestStream(ServiceMessage request);

  /**
   * Fire and Forget interaction model of {@code ServiceMessage}.
   *
   * @param request ServiceMessage.
   * @return {@code Publisher} that completes when the passed {@code request} is successfully handled, otherwise errors.
   */
  Mono<Void> fireAndForget(ServiceMessage request);

  /**
   * Request-Stream interaction model of.
   *
   * @param ServiceMessage Request.
   * @return {@code Publisher} containing the stream of {@code ServiceMessage}s representing the response.
   */
  public Flux<ServiceMessage> requestChannel(Flux<ServiceMessage> request);

}
