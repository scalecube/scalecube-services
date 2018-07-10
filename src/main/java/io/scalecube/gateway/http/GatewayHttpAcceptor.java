package io.scalecube.gateway.http;

import static io.netty.handler.codec.http.HttpHeaderNames.ALLOW;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.TEXT_PLAIN;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.NullData;
import io.scalecube.services.api.ServiceMessage;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.server.HttpServerRequest;
import reactor.ipc.netty.http.server.HttpServerResponse;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpContent;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.BiFunction;

public class GatewayHttpAcceptor implements BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(GatewayHttpAcceptor.class);

  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);

  private final ServiceCall serviceCall;

  GatewayHttpAcceptor(ServiceCall serviceCall) {
    this.serviceCall = serviceCall;
  }

  @Override
  public Publisher<Void> apply(HttpServerRequest httpServerRequest, HttpServerResponse httpServerResponse) {
    if (httpServerRequest.method() != POST) {
      LOGGER.error("Unsupported HTTP method. Expected POST, actual {}", httpServerRequest.method());
      return methodNotAllowed(httpServerResponse);
    }

    final Flux<ByteBuf> dataStream = httpServerRequest.receiveContent()
        .flatMap(httpContent -> callService(httpServerRequest.uri(), httpContent));

    return dataStream
        .timeout(DEFAULT_TIMEOUT)
        .flatMap(byteBuf -> ok(httpServerResponse, byteBuf))
        .onErrorResume(throwable -> internalServerError(httpServerResponse, throwable.getMessage()));
  }

  private Mono<ByteBuf> callService(String qualifier, HttpContent httpContent) {
    final ServiceMessage.Builder serviceMessage = ServiceMessage.builder().qualifier(qualifier);

    if (httpContent != null) {
      serviceMessage.data(httpContent.content().slice().retain());
    }

    final Mono<ServiceMessage> requestOneResult = serviceCall.requestOne(serviceMessage.build());

    return requestOneResult.flatMap(message -> {
      if (message.data().equals(NullData.NULL_DATA)) {
        return Mono.empty();
      }

      return message.data();
    });
  }

  private Publisher<Void> methodNotAllowed(HttpServerResponse response) {
    return response
        .addHeader(ALLOW, POST.name())
        .status(METHOD_NOT_ALLOWED)
        .send();
  }

  private Publisher<Void> ok(HttpServerResponse httpServerResponse, ByteBuf byteBuf) {
    return httpServerResponse
        .status(OK)
        .sendObject(byteBuf);
  }

  private Publisher<Void> internalServerError(HttpServerResponse response, String message) {
    return response
        .addHeader(CONTENT_TYPE, TEXT_PLAIN)
        .status(INTERNAL_SERVER_ERROR)
        .sendString(Mono.just(message));
  }

}
