package io.scalecube.services.gateway.transport;

import io.scalecube.services.api.ServiceMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface GatewayClient {

  /**
   * Communication mode that gives single response to single request.
   *
   * @param request request message.
   * @return Publisher that emits single response form remote server as it's ready.
   */
  Mono<ServiceMessage> requestResponse(ServiceMessage request);

  /**
   * Communication mode that gives stream of responses to single request.
   *
   * @param request request message.
   * @return Publisher that emits responses from remote server.
   */
  Flux<ServiceMessage> requestStream(ServiceMessage request);

  /**
   * Communication mode that gives stream of responses to stream of requests.
   *
   * @param requests request stream.
   * @return Publisher that emits responses from remote server.
   */
  Flux<ServiceMessage> requestChannel(Flux<ServiceMessage> requests);

  /**
   * Initiate cleaning of underlying resources (if any) like closing websocket connection or rSocket
   * session. Subsequent calls of requestOne() or requestMany() must issue new connection creation.
   * Note that close is not the end of client lifecycle.
   */
  void close();

  /**
   * Return close completion signal of the gateway client.
   *
   * @return close completion signal
   */
  Mono<Void> onClose();
}
