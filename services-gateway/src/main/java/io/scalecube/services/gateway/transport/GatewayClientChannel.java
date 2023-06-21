package io.scalecube.services.gateway.transport;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.gateway.ServiceMessageCodec;
import io.scalecube.services.transport.api.ClientChannel;
import java.lang.reflect.Type;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class GatewayClientChannel implements ClientChannel {

  private final GatewayClient gatewayClient;

  GatewayClientChannel(GatewayClient gatewayClient) {
    this.gatewayClient = gatewayClient;
  }

  @Override
  public Mono<ServiceMessage> requestResponse(ServiceMessage clientMessage, Type responseType) {
    return gatewayClient
        .requestResponse(clientMessage)
        .map(msg -> ServiceMessageCodec.decodeData(msg, responseType));
  }

  @Override
  public Flux<ServiceMessage> requestStream(ServiceMessage clientMessage, Type responseType) {
    return gatewayClient
        .requestStream(clientMessage)
        .map(msg -> ServiceMessageCodec.decodeData(msg, responseType));
  }

  @Override
  public Flux<ServiceMessage> requestChannel(
      Publisher<ServiceMessage> clientMessageStream, Type responseType) {
    return gatewayClient
        .requestChannel(Flux.from(clientMessageStream))
        .map(msg -> ServiceMessageCodec.decodeData(msg, responseType));
  }
}
