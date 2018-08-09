package io.scalecube.gateway.clientsdk.websocket;

import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.ClientTransport;
import io.scalecube.gateway.clientsdk.codec.ClientMessageCodec;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.resources.LoopResources;

import io.rsocket.transport.netty.WebsocketDuplexConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public final class WSocketClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(WSocketClientTransport.class);

  private final ClientSettings settings;
  private final ClientMessageCodec messageCodec;
  private final HttpClient httpClient;

  public WSocketClientTransport(ClientSettings settings,
      ClientMessageCodec messageCodec,
      LoopResources loopResources) {

    this.settings = settings;
    this.messageCodec = messageCodec;

    // init HttpClient

    httpClient = HttpClient.create(options -> options.disablePool()
        .connectAddress(() -> InetSocketAddress.createUnresolved(settings.host(), settings.port()))
        .loopResources(loopResources));
  }

  @Override
  public Mono<ClientMessage> requestResponse(ClientMessage request) {
    ClientMessageCodec cmc;
    return null;
  }

  @Override
  public Flux<ClientMessage> requestStream(ClientMessage request) {
    return null;
  }

  @Override
  public Mono<Void> close() {
    return null;
  }

  public Mono<WebsocketDuplexConnection> connect() {
    return Mono.create(sink -> httpClient.ws("/").flatMap(
        response -> response.receiveWebsocket((in, out) -> {
          WebsocketDuplexConnection connection = new WebsocketDuplexConnection(in, out, in.context());
          sink.success(connection);
          return connection.onClose();
        }))
        .doOnError(sink::error)
        .subscribe());
  }
}
