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
import io.netty.handler.codec.http.HttpResponseStatus;
import io.scalecube.gateway.GatewayMetrics;
import io.scalecube.gateway.ReferenceCountUtil;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.DataCodec;
import io.scalecube.services.exceptions.ExceptionProcessor;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
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

    if (httpRequest.method() != POST) {
      LOGGER.error("Unsupported HTTP method. Expected POST, actual {}", httpRequest.method());
      return methodNotAllowed(httpResponse);
    }

    return httpRequest
        .receive()
        .map(ByteBuf::retain)
        .doOnNext(content -> metrics.markRequest())
        .flatMap(content -> handleRequest(content, httpRequest, httpResponse))
        .doOnComplete(metrics::markResponse)
        .timeout(DEFAULT_TIMEOUT)
        .onErrorResume(t -> error(httpResponse, ExceptionProcessor.toMessage(t)));
  }

  private Mono<Void> handleRequest(
      ByteBuf content, HttpServerRequest httpRequest, HttpServerResponse httpResponse) {

    ServiceMessage.Builder messageBuilder =
        ServiceMessage.builder()
            .header("gw-recv-from-client-time", String.valueOf(System.currentTimeMillis()))
            .qualifier(httpRequest.uri());

    return serviceCall
        .requestOne(messageBuilder.data(content).build())
        .switchIfEmpty(Mono.defer(() -> Mono.just(messageBuilder.data(null).build())))
        .flatMap(
            response -> {
              enrichResponse(httpRequest, httpResponse, response);
              return Mono.defer(
                  () ->
                      ExceptionProcessor.isError(response) // check error
                          ? error(httpResponse, response)
                          : response.hasData() // check data
                              ? ok(httpResponse, response)
                              : noContent(httpResponse));
            });
  }

  private Publisher<Void> methodNotAllowed(HttpServerResponse httpResponse) {
    return httpResponse.addHeader(ALLOW, POST.name()).status(METHOD_NOT_ALLOWED).send();
  }

  private Mono<Void> error(HttpServerResponse httpResponse, ServiceMessage response) {
    int code = Integer.parseInt(Qualifier.getQualifierAction(response.qualifier()));
    HttpResponseStatus status = HttpResponseStatus.valueOf(code);

    ByteBuf content =
        response.hasData(ErrorData.class)
            ? encodeData(response.data(), response.dataFormatOrDefault())
            : response.data();

    return httpResponse.status(status).sendObject(content).then();
  }

  private Mono<Void> noContent(HttpServerResponse httpResponse) {
    return httpResponse.status(NO_CONTENT).send();
  }

  private Mono<Void> ok(HttpServerResponse httpResponse, ServiceMessage response) {
    ByteBuf content =
        response.hasData(ByteBuf.class)
            ? response.data()
            : encodeData(response.data(), response.dataFormatOrDefault());

    return httpResponse.status(OK).sendObject(content).then();
  }

  private ByteBuf encodeData(Object data, String dataFormat) {
    ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();

    try {
      DataCodec.getInstance(dataFormat).encode(new ByteBufOutputStream(byteBuf), data);
    } catch (IOException e) {
      ReferenceCountUtil.safestRelease(byteBuf);
      LOGGER.error("Failed to encode data: {}", data, e);
      return Unpooled.EMPTY_BUFFER;
    }

    return byteBuf;
  }

  private void enrichResponse(
      HttpServerRequest httpRequest, HttpServerResponse httpResponse, ServiceMessage response) {

    Optional.ofNullable(httpRequest.requestHeaders().get("client-send-time"))
        .ifPresent(s -> httpResponse.header("client-send-time", s));

    Optional.ofNullable(response.header("service-recv-time"))
        .ifPresent(s -> httpResponse.header("service-recv-time", s));

    Optional.ofNullable(response.header("gw-recv-from-client-time"))
        .ifPresent(s -> httpResponse.header("gw-recv-from-client-time", s));

    httpResponse.header("gw-recv-from-service-time", String.valueOf(System.currentTimeMillis()));
  }
}
