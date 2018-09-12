package io.scalecube.gateway.http;

import static io.netty.handler.codec.http.HttpHeaderNames.ALLOW;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.DataCodec;
import io.scalecube.services.exceptions.ExceptionProcessor;
import java.io.IOException;
import java.time.Duration;
import java.util.function.BiFunction;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.server.HttpServerRequest;
import reactor.ipc.netty.http.server.HttpServerResponse;

public class GatewayHttpAcceptor
    implements BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(GatewayHttpAcceptor.class);

  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);

  private final ServiceCall serviceCall;

  GatewayHttpAcceptor(ServiceCall serviceCall) {
    this.serviceCall = serviceCall;
  }

  @Override
  public Publisher<Void> apply(HttpServerRequest httpRequest, HttpServerResponse httpResponse) {
    if (httpRequest.method() != POST) {
      LOGGER.error("Unsupported HTTP method. Expected POST, actual {}", httpRequest.method());
      return methodNotAllowed(httpResponse);
    }

    return httpRequest
        .receiveContent()
        .flatMap(httpContent -> handleHttpContent(httpContent, httpResponse, httpRequest))
        .timeout(DEFAULT_TIMEOUT)
        .onErrorResume(throwable -> error(httpResponse, throwable));
  }

  private Mono<Void> handleHttpContent(
      HttpContent httpContent, HttpServerResponse httpResponse, HttpServerRequest httpRequest) {
    long gwRecvFromClientTime = System.currentTimeMillis();

    ServiceMessage.Builder serviceMessage = ServiceMessage.builder().qualifier(httpRequest.uri());

    try {
      serviceMessage.data(httpContent.content().slice().retain());
      return serviceCall
          .requestOne(serviceMessage.build())
          .switchIfEmpty(Mono.defer(() -> Mono.just(serviceMessage.data(null).build())))
          .flatMap(
              message -> {
                enrichHttpHeaders(httpResponse, httpRequest, gwRecvFromClientTime, message);
                return Mono.from(toHttpResponse(httpResponse, message));
              });
    } catch (Exception e) {
      ReferenceCountUtil.safeRelease(httpContent);
      return Mono.error(e);
    }
  }

  private Publisher<Void> toHttpResponse(
      HttpServerResponse response, ServiceMessage serviceMessage) {
    if (ExceptionProcessor.isError(serviceMessage)) {
      return error(response, serviceMessage);
    }

    if (!serviceMessage.hasData()) {
      return noContent(response);
    }

    return ok(response, serviceMessage.data());
  }

  private Publisher<Void> methodNotAllowed(HttpServerResponse response) {
    return response.addHeader(ALLOW, POST.name()).status(METHOD_NOT_ALLOWED).send();
  }

  private Publisher<Void> error(HttpServerResponse response, Throwable throwable) {
    return error(response, ExceptionProcessor.toMessage(throwable));
  }

  private Publisher<Void> error(HttpServerResponse response, ServiceMessage serviceMessage) {
    int code = Integer.parseInt(Qualifier.getQualifierAction(serviceMessage.qualifier()));

    ByteBuf body;
    if (serviceMessage.hasData(ErrorData.class)) {
      body = encodeErrorData(serviceMessage.data());
    } else {
      body = serviceMessage.data();
    }

    return response.status(HttpResponseStatus.valueOf(code)).sendObject(body);
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

  private Publisher<Void> noContent(HttpServerResponse response) {
    return response.status(NO_CONTENT).send();
  }

  private Publisher<Void> ok(HttpServerResponse response, ByteBuf body) {
    return response.status(OK).sendObject(body);
  }

  private void enrichHttpHeaders(
      HttpServerResponse httpResponse,
      HttpServerRequest httpRequest,
      long gwRecvFromClientTime,
      ServiceMessage message) {
    String clientSendTime = httpRequest.requestHeaders().get("client-send-time");
    if (clientSendTime != null) {
      httpResponse.header("client-send-time", clientSendTime);
    }

    String serviceRecvTime = message.header("service-recv-time");
    if (serviceRecvTime != null) {
      httpResponse.header("service-recv-time", serviceRecvTime);
    }

    httpResponse
        .responseHeaders()
        .set("gw-recv-from-client-time", gwRecvFromClientTime)
        .set("gw-recv-from-service-time", System.currentTimeMillis());
  }
}
