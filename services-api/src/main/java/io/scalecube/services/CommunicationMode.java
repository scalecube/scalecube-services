package io.scalecube.services;

public enum CommunicationMode {

  /**
   * Corresponds to {@code Mono<Response> action(Request)} or {@code Mono<Void> action(Request)}.
   */
  REQUEST_RESPONSE,

  /** Corresponds to {@code Flux<OutputData> action(Request)}. */
  REQUEST_STREAM,

  /** Corresponds to {@code Flux<OutputData> action(Flux<InputData>)}. */
  REQUEST_CHANNEL;
}
