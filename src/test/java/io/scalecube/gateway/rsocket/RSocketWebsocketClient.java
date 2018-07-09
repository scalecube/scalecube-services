package io.scalecube.gateway.rsocket;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.HeadersCodec;
import io.scalecube.services.codec.ServiceMessageCodec;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.util.ByteBufPayload;

import org.reactivestreams.Publisher;

import java.net.InetSocketAddress;

public class RSocketWebsocketClient {

  private static final String DEFAULT_CONTENT_TYPE = "application/json";

  private static final ServiceMessageCodec CODEC =
      new ServiceMessageCodec(HeadersCodec.getInstance(DEFAULT_CONTENT_TYPE));

  private RSocket client;

  public RSocketWebsocketClient(InetSocketAddress gatewayAddress) {
    WebsocketClientTransport transport = WebsocketClientTransport.create(gatewayAddress);
    client = RSocketFactory.connect().keepAlive().transport(transport).start().block();
  }

  public Mono<Void> fireAndForget(ServiceMessage message) {
    return client.fireAndForget(toPayload(message));
  }

  public Mono<ServiceMessage> requestResponse(ServiceMessage message) {
    return client.requestResponse(toPayload(message)).map(this::toServiceMessage);
  }

  public Flux<ServiceMessage> requestStream(ServiceMessage message) {
    return client.requestStream(toPayload(message)).map(this::toServiceMessage);
  }

  public Flux<ServiceMessage> requestChannel(Publisher<ServiceMessage> messages) {
    Flux<Payload> payloads = Flux.from(messages).map(this::toPayload);
    return client.requestChannel(payloads).map(this::toServiceMessage);
  }

  private Payload toPayload(ServiceMessage serviceMessage) {
    return CODEC.encodeAndTransform(serviceMessage, ByteBufPayload::create);
  }

  private ServiceMessage toServiceMessage(Payload payload) {
    return CODEC.decode(payload.sliceData(), payload.sliceMetadata());
  }

}
