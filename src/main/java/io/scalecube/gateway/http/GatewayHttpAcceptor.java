package io.scalecube.gateway.http;

import static io.netty.handler.codec.http.HttpHeaderNames.ALLOW;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.DataCodec;
import io.scalecube.services.exceptions.ExceptionProcessor;

import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.server.HttpServerRequest;
import reactor.ipc.netty.http.server.HttpServerResponse;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

    return httpServerRequest.receiveContent()
        .flatMap(httpContent -> callService(httpServerRequest.uri(), httpContent, httpServerResponse))
        .timeout(DEFAULT_TIMEOUT);
  }

  private Mono<Void> callService(String qualifier, HttpContent httpContent, HttpServerResponse httpServerResponse) {
    final ServiceMessage.Builder serviceMessage = ServiceMessage.builder().qualifier(qualifier);
    try {
      serviceMessage.data(httpContent.content().slice().retain());
      return serviceCall.requestOne(serviceMessage.build())
          .flatMap(msg -> toHttpResponse(httpServerResponse, msg))
          .switchIfEmpty(Mono.defer(() -> noContent(httpServerResponse)))
          .onErrorResume(throwable -> error(httpServerResponse, throwable));
    } catch (Exception e) {
      ReferenceCountUtil.safeRelease(httpContent);
      return Mono.error(e);
    }
  }

  private Mono<Void> toHttpResponse(HttpServerResponse response, ServiceMessage serviceMessage) {
    if (ExceptionProcessor.isError(serviceMessage)) {
      return error(response, serviceMessage);
    }
    return ok(response, serviceMessage.data());
  }

  private Publisher<Void> methodNotAllowed(HttpServerResponse response) {
    return response
        .addHeader(ALLOW, POST.name())
        .status(METHOD_NOT_ALLOWED)
        .send();
  }

  private Mono<Void> error(HttpServerResponse response, Throwable throwable) {
    return error(response, ExceptionProcessor.toMessage(throwable));
  }

  private Mono<Void> error(HttpServerResponse response, ServiceMessage serviceMessage) {
    int code = Integer.parseInt(Qualifier.getQualifierAction(serviceMessage.qualifier()));

    ByteBuf body;
    if (serviceMessage.hasData(ErrorData.class)) {
      body = encodeErrorData(serviceMessage.data());
    } else {
      body = serviceMessage.data();
    }

    return response
        .status(HttpResponseStatus.valueOf(code))
        .sendObject(body).then();
  }

  private ByteBuf encodeErrorData(ErrorData errorData) {
    ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
    try {
      DataCodec.getInstance("application/json").encode(new ByteBufOutputStream(byteBuf), errorData);
    } catch (IOException e) {
      ReferenceCountUtil.safeRelease(byteBuf);
      LOGGER.error("Failed to encode data: {}", errorData, e);
      return Unpooled.EMPTY_BUFFER;
    }

    return byteBuf;
  }

  private Mono<Void> noContent(HttpServerResponse response) {
    return response
        .status(NO_CONTENT)
        .send();
  }

  private Mono<Void> ok(HttpServerResponse response, ByteBuf body) {
    return response
        .status(OK)
        .sendObject(body).then();
  }

}
