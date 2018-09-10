package io.scalecube.gateway.clientsdk.http;

import io.netty.buffer.ByteBuf;
import io.scalecube.gateway.clientsdk.ClientCodec;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.ClientTransport;
import io.scalecube.services.api.Qualifier;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.resources.LoopResources;

public final class HttpClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientTransport.class);

  private final InetSocketAddress address;
  private final ClientCodec<ByteBuf> codec;
  private final HttpClient httpClient;

  /**
   * Creates instance of http client transport.
   *
   * @param settings client settings
   * @param codec client message codec
   * @param loopResources loop resources
   */
  public HttpClientTransport(
      ClientSettings settings, ClientCodec<ByteBuf> codec, LoopResources loopResources) {
    this.codec = codec;

    address = InetSocketAddress.createUnresolved(settings.host(), settings.port());

    httpClient =
        HttpClient.create(
            options ->
                options.disablePool().connectAddress(() -> address).loopResources(loopResources));
  }

  @Override
  public Mono<ClientMessage> requestResponse(ClientMessage request) {
    return Mono.defer(
        () ->
            httpClient
                .post(
                    request.qualifier(),
                    httpRequest -> {
                      LOGGER.info("Sending request {}", request);

                      return httpRequest
                          .failOnClientError(false)
                          .failOnServerError(false)
                          .send(Mono.just(codec.encode(request)));
                    })
                .flatMap(
                    httpResponse ->
                        httpResponse
                            .receiveContent()
                            .map(
                                httpContent -> {
                                  int httpCode = httpResponse.status().code();

                                  String qualifier =
                                      isError(httpCode)
                                          ? Qualifier.asError(httpCode)
                                          : request.qualifier();

                                  ClientMessage message =
                                      ClientMessage.builder()
                                          .qualifier(qualifier)
                                          .data(httpContent.content().slice().retain())
                                          .build();

                                  LOGGER.info("Received response {}", message);
                                  return message;
                                })
                            .singleOrEmpty()));
  }

  @Override
  public Flux<ClientMessage> requestStream(ClientMessage request) {
    throw new UnsupportedOperationException("Request stream is not supported by HTTP/1.x");
  }

  @Override
  public Mono<Void> close() {
    return Mono.empty();
  }

  private boolean isError(int httpCode) {
    int httpCodeType = httpCode / 100;
    return httpCodeType == 4 || httpCodeType == 5;
  }
}
