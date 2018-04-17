package io.scalecube.services.transport.rsocket.client;

import io.rsocket.RSocket;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.transport.client.api.ClientChannel;
import io.scalecube.services.transport.rsocket.PayloadCodec;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RSocketServiceClientAdapter implements ClientChannel {

  private RSocket rSocket;
  private PayloadCodec payloadCodec;

  public RSocketServiceClientAdapter(RSocket rSocket, PayloadCodec payloadCodec) {
    this.rSocket = rSocket;
    this.payloadCodec = payloadCodec;
  }

  @Override
  public Mono<ServiceMessage> requestResponse(ServiceMessage request) {
    return rSocket.requestResponse(payloadCodec.encode(request)).map(payloadCodec::decode);
  }

  @Override
  public Flux<ServiceMessage> requestStream(ServiceMessage request) {
    return rSocket.requestStream(payloadCodec.encode(request)).map(payloadCodec::decode);
  }

  @Override
  public Mono<Void> fireAndForget(ServiceMessage request) {
    return rSocket.fireAndForget(payloadCodec.encode(request));
  }

  @Override
  public Flux<ServiceMessage> requestChannel(Flux<ServiceMessage> request) {
    return rSocket.requestChannel(request.map(payloadCodec::encode)).map(payloadCodec::decode);
  }

}
