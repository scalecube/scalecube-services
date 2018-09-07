package io.scalecube.gateway.websocket;

import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.codec.WebsocketGatewayMessageCodec;
import io.scalecube.gateway.clientsdk.websocket.WebsocketClientTransport;
import io.scalecube.gateway.examples.GreetingService;
import io.scalecube.services.codec.DataCodec;
import reactor.ipc.netty.resources.LoopResources;

public class WebsocketClientTransportTest {

  public static void main(String[] args) throws InterruptedException {
    String contentType = "application/json";
    ClientSettings settings =
        ClientSettings.builder().contentType(contentType).host("localhost").port(7070).build();
    LoopResources loopResources = LoopResources.create("worker");

    WebsocketGatewayMessageCodec codec =
        new WebsocketGatewayMessageCodec(DataCodec.getInstance(contentType));

    WebsocketClientTransport transport =
        new WebsocketClientTransport(settings, codec, loopResources);

    Client client = new Client(transport, codec);
    client
        .forService(GreetingService.class)
        .failingOne("hello")
        .subscribe(
            null,
            throwable -> {
              System.out.println(throwable.hashCode());
            },
            () -> System.out.println("complete"));

    Thread.currentThread().join();
  }
}
