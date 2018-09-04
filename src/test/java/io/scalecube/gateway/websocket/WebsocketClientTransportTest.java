package io.scalecube.gateway.websocket;

import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.codec.WebsocketGatewayMessageCodec;
import io.scalecube.gateway.clientsdk.websocket.WebsocketClientTransport;
import io.scalecube.services.codec.DataCodec;
import java.time.Duration;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.resources.LoopResources;

public class WebsocketClientTransportTest {

  public static void main(String[] args) {
    String contentType = "application/json";
    ClientSettings settings =
        ClientSettings.builder().contentType(contentType).host("localhost").port(7070).build();
    LoopResources loopResources = LoopResources.create("worker");

    WebsocketClientTransport transport =
        new WebsocketClientTransport(
            settings,
            new WebsocketGatewayMessageCodec(DataCodec.getInstance(contentType)),
            loopResources);

    ClientMessage request = ClientMessage.builder().qualifier("/greeting/one").build();

    Mono.delay(Duration.ofSeconds(10))
        .doOnTerminate(
            () -> {
              System.err.println("closing ...");
              transport.close().subscribe();
            })
        .subscribe();

    ClientMessage block = transport.requestResponse(request).block();

    System.out.println(block);
  }
}
