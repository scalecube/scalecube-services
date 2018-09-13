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
import io.scalecube.gateway.GatewayMetrics;
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
  private final GatewayMetrics metrics;

  GatewayHttpAcceptor(ServiceCall serviceCall, GatewayMetrics metrics) {
    this.serviceCall = serviceCall;
    this.metrics = metrics;
  }

  @Override
  public Publisher<Void> apply(HttpServerRequest httpRequest, HttpServerResponse httpResponse) {
    LOGGER.debug(
        "Accepted request: {}, headers: {}, params: {}",
        httpRequest,
        httpRequest.requestHeaders(),
        httpRequest.params());
    metrics.incConnection();

    httpResponse.context().onClose(metrics::decConnection);

    if (httpRequest.method() != POST) {
      LOGGER.error("Unsupported HTTP method. Expected POST, actual {}", httpRequest.method());
      return methodNotAllowed(httpResponse);
    }

    return httpRequest
        .receiveContent()
        .doOnNext(input -> metrics.markRequest())
        .flatMap(httpContent -> handleHttpContent(httpContent, httpResponse, httpRequest))
        .doOnComplete(metrics::markResponse)
        .timeout(DEFAULT_TIMEOUT)
        .onErrorResume(throwable -> error(httpResponse, throwable));
  }

  private Mono<Void> handleHttpContent(
      HttpContent httpContent, HttpServerResponse httpResponse, HttpServerRequest httpRequest) {
    LOGGER.debug("Try to handle content: {}", httpRequest, httpContent);

    long gwRecvFromClientTime = System.currentTimeMillis();

    ServiceMessage.Builder messageBuilder = ServiceMessage.builder().qualifier(httpRequest.uri());

    try {
      ByteBuf dataByteBuf = httpContent.content().slice().retain();

      return serviceCall
          .requestOne(messageBuilder.data(dataByteBuf).build())
          .switchIfEmpty(Mono.defer(() -> Mono.just(messageBuilder.data(null).build())))
          .flatMap(
              message -> {
                enrichHttpHeaders(httpResponse, httpRequest, gwRecvFromClientTime, message);
                return Mono.from(toHttpResponse(httpResponse, message));
              });
    } catch (Exception e) {
      ReferenceCountUtil.safeRelease(httpContent);
      LOGGER.error(
          "Error during handling request: {}, headers: {}, params: {}",
          httpRequest,
          httpRequest.requestHeaders(),
          httpRequest.params(),
          e);
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

    return ok(response, serviceMessage);
  }

  private Publisher<Void> methodNotAllowed(HttpServerResponse response) {
    return response.addHeader(ALLOW, POST.name()).status(METHOD_NOT_ALLOWED).send();
  }

  private Publisher<Void> error(HttpServerResponse response, Throwable throwable) {
    return error(response, ExceptionProcessor.toMessage(throwable));
  }

  private Publisher<Void> error(HttpServerResponse response, ServiceMessage message) {
    int code = Integer.parseInt(Qualifier.getQualifierAction(message.qualifier()));

    ByteBuf body;
    if (message.hasData(ErrorData.class)) {
      body = encodeData(message.data());
    } else {
      body = message.data();
    }

    return response.status(HttpResponseStatus.valueOf(code)).sendObject(body);
  }

  private Publisher<Void> noContent(HttpServerResponse response) {
    return response.status(NO_CONTENT).send();
  }

  private Publisher<Void> ok(HttpServerResponse response, ServiceMessage message) {
    ByteBuf body;

    if (message.hasData(ByteBuf.class)) {
      body = message.data();
    } else {
      body = encodeData(message.data());
    }

    return response.status(OK).sendObject(body);
  }

  private ByteBuf encodeData(Object data) {
    ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();

    try {
      DataCodec.getInstance("application/json").encode(new ByteBufOutputStream(byteBuf), data);
    } catch (IOException e) {
      ReferenceCountUtil.safeRelease(byteBuf);
      LOGGER.error("Failed to encode data: {}", data, e);
      return Unpooled.EMPTY_BUFFER;
    }

    return byteBuf;
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
