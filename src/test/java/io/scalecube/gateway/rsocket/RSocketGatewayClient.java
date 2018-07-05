package io.scalecube.gateway.rsocket;

import io.scalecube.gateway.core.GatewayMessage;
import io.scalecube.gateway.rsocket.core.RSocketGatewayMessageCodec;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.util.ByteBufPayload;

import org.reactivestreams.Publisher;

import java.net.InetSocketAddress;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RSocketGatewayClient {

  private static final RSocketGatewayMessageCodec CODEC = new RSocketGatewayMessageCodec();

  private RSocket client;

  public RSocketGatewayClient(InetSocketAddress gatewayAddress) {
    WebsocketClientTransport transport = WebsocketClientTransport.create(gatewayAddress);
    client = RSocketFactory.connect().keepAlive().transport(transport).start().block();
  }

  public Mono<Void> fireAndForget(GatewayMessage message) {
    return client.fireAndForget(toPayload(message));
  }

  public Mono<GatewayMessage> requestResponse(GatewayMessage message) {
    return client.requestResponse(toPayload(message)).map(this::toGatewayMessage);
  }

  public Flux<GatewayMessage> requestStream(GatewayMessage message) {
    return client.requestStream(toPayload(message)).map(this::toGatewayMessage);
  }

  public Flux<GatewayMessage> requestChannel(Publisher<GatewayMessage> messages) {
    Flux<Payload> payloads = Flux.from(messages).map(this::toPayload);
    return client.requestChannel(payloads).map(this::toGatewayMessage);
  }

  private Payload toPayload(GatewayMessage gatewayMessage) {
    //TODO: do not use RSocketGatewayMessageCodec here
    return ByteBufPayload.create(CODEC.encode(gatewayMessage));
  }

  private GatewayMessage toGatewayMessage(Payload payload) {
    return CODEC.decode(payload);
  }

}
