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

  private static final String SERVICE_RECV_TIME = "service-recv-time";
  private static final String SERVICE_SEND_TIME = "service-send-time";
  private static final String CLIENT_RECV_TIME = "client-recv-time";
  private static final String CLIENT_SEND_TIME = "client-send-time";

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
        .aggregate()
        .map(ByteBuf::retain)
        .doOnNext(content -> metrics.markRequest())
        .flatMap(content -> handleRequest(content, httpRequest, httpResponse))
        .doOnTerminate(metrics::markResponse)
        .onErrorResume(t -> error(httpResponse, ExceptionProcessor.toMessage(t)));
  }

  private Mono<Void> handleRequest(
      ByteBuf content, HttpServerRequest httpRequest, HttpServerResponse httpResponse) {

    String qualifier = httpRequest.uri();

    return serviceCall
        .requestOne(ServiceMessage.builder().qualifier(qualifier).data(content).build())
        .switchIfEmpty(
            Mono.defer(() -> Mono.just(ServiceMessage.builder().qualifier(qualifier).build())))
        .flatMap(
            response -> {
              enrichResponse(httpResponse, response);
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
    } catch (Throwable t) {
      ReferenceCountUtil.safestRelease(byteBuf);
      LOGGER.error("Failed to encode data: {}", data, t);
      return Unpooled.EMPTY_BUFFER;
    }

    return byteBuf;
  }

  private void enrichResponse(HttpServerResponse httpResponse, ServiceMessage response) {
    Optional.ofNullable(response.header(CLIENT_SEND_TIME))
        .ifPresent(value -> httpResponse.header(CLIENT_SEND_TIME, value));

    Optional.ofNullable(response.header(CLIENT_RECV_TIME))
        .ifPresent(value -> httpResponse.header(CLIENT_RECV_TIME, value));

    Optional.ofNullable(response.header(SERVICE_RECV_TIME))
        .ifPresent(value -> httpResponse.header(SERVICE_RECV_TIME, value));

    Optional.ofNullable(response.header(SERVICE_SEND_TIME))
        .ifPresent(value -> httpResponse.header(SERVICE_SEND_TIME, value));
  }
}
