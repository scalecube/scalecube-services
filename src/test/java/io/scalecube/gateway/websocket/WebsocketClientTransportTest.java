package io.scalecube.gateway.websocket;

import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.codec.WebsocketGatewayMessageCodec;
import io.scalecube.gateway.clientsdk.websocket.WebsocketClientTransport;
import io.scalecube.services.codec.DataCodec;
import reactor.ipc.netty.resources.LoopResources;

public class WebsocketClientTransportTest {

  public static void main(String[] args) throws InterruptedException {
    String contentType = "application/json";
    ClientSettings settings =
        ClientSettings.builder().contentType(contentType).host("localhost").port(7070).build();
    LoopResources loopResources = LoopResources.create("worker");

    WebsocketClientTransport transport =
        new WebsocketClientTransport(
            settings,
            new WebsocketGatewayMessageCodec(DataCodec.getInstance(contentType)),
            loopResources);

    ClientMessage request =
        ClientMessage.builder().qualifier("/greeting/one").header("sid", "1").build();

    //    Mono.delay(Duration.ofSeconds(15))
    //        .doOnTerminate(
    //            () -> {
    //              System.err.println("### Closing ...");
    //              transport.close().subscribe();
    //            })
    //        .subscribe();

    transport
        .requestResponse(request)
        .subscribe(
            System.out::println, System.err::println, () -> System.out.println("### Complete"));

    Thread.currentThread().join();
  }
}
