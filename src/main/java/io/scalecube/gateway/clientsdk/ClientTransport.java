package io.scalecube.gateway.clientsdk;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ClientTransport {

  /**
   * Communication mode that gives single response to single request.
   *
   * @param request request message.
   * @return Publisher that emits single response form remote server as it's ready.
   */
  Mono<ClientMessage> requestResponse(ClientMessage request);

  /**
   * Communication mode that gives stream of responses to single request.
   *
   * @param request request message.
   * @return Publisher that emits responses from remote server.
   */
  Flux<ClientMessage> requestStream(ClientMessage request);

  /**
   * Initiate cleaning of underlaying resources (if any) like closing websocket connection or
   * rSocket session. Subsequent calls of requestOne() or requestMany() must issue new connection
   * creation. Note that close is not the end of client lifecycle.
   *
   * @return Async completion signal.
   */
  Mono<Void> close();
}
