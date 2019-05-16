package io.scalecube.services.gateway.http;

import static io.netty.handler.codec.http.HttpHeaderNames.ALLOW;
import static io.netty.handler.codec.http.HttpMethod.OPTIONS;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.api.ServiceMessage.Builder;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.gateway.GatewayMetrics;
import io.scalecube.services.gateway.ReferenceCountUtil;
import io.scalecube.services.transport.api.DataCodec;
import java.util.Optional;
import java.util.function.BiFunction;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufMono;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

public class HttpGatewayAcceptor
    implements BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpGatewayAcceptor.class);

  private static final String SERVICE_RECV_TIME = "service-recv-time";
  private static final String SERVICE_SEND_TIME = "service-send-time";
  private static final String CLIENT_RECV_TIME = "client-recv-time";
  private static final String CLIENT_SEND_TIME = "client-send-time";

  private final ServiceCall serviceCall;
  private final GatewayMetrics metrics;

  HttpGatewayAcceptor(ServiceCall serviceCall, GatewayMetrics metrics) {
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

    if (httpRequest.method() == OPTIONS) {
      return crossOriginResourceSharing(httpResponse).status(OK).send().then();
    }

    if (httpRequest.method() != POST) {
      LOGGER.error("Unsupported HTTP method. Expected POST, actual {}", httpRequest.method());
      return methodNotAllowed(httpResponse);
    }

    return httpRequest
        .receive()
        .aggregate()
        .switchIfEmpty(Mono.defer(() -> ByteBufMono.just(Unpooled.EMPTY_BUFFER)))
        .map(ByteBuf::retain)
        .doOnNext(content -> metrics.markRequest())
        .flatMap(content -> handleRequest(content, httpRequest, httpResponse))
        .doOnSuccess(avoid -> metrics.markResponse())
        .onErrorResume(t -> error(httpResponse, DefaultErrorMapper.INSTANCE.toMessage(t)));
  }

  /**
   * enable CORS, cross origin resource sharing.
   *
   * @return HttpServerResponse with CORS.
   */
  private HttpServerResponse crossOriginResourceSharing(HttpServerResponse httpResponse) {
    httpResponse.header("Access-Control-Allow-Origin", "*");
    httpResponse.header("Access-Control-Allow-Headers", "*");
    httpResponse.header("Access-Control-Allow-Methods", "POST");
    return httpResponse;
  }

  private Mono<Void> handleRequest(
      ByteBuf content, HttpServerRequest httpRequest, HttpServerResponse httpResponse) {

    String qualifier = httpRequest.uri();
    Builder builder = ServiceMessage.builder().qualifier(qualifier).data(content);
    enrichRequest(httpRequest.requestHeaders(), builder);

    return serviceCall
        .requestOne(builder.build())
        .doOnNext(message -> metrics.markServiceResponse())
        .switchIfEmpty(
            Mono.defer(() -> Mono.just(ServiceMessage.builder().qualifier(qualifier).build())))
        .flatMap(
            response -> {
              enrichResponse(httpResponse, response);
              return Mono.defer(
                  () ->
                      response.isError() // check error
                          ? error(httpResponse, response)
                          : response.hasData() // check data
                              ? ok(httpResponse, response)
                              : noContent(httpResponse));
            });
  }

  private Publisher<Void> methodNotAllowed(HttpServerResponse httpResponse) {
    httpResponse.header("Access-Control-Allow-Origin", "*");
    return httpResponse.addHeader(ALLOW, POST.name()).status(METHOD_NOT_ALLOWED).send();
  }

  private Mono<Void> error(HttpServerResponse httpResponse, ServiceMessage response) {
    int code = response.errorType();
    HttpResponseStatus status = HttpResponseStatus.valueOf(code);

    ByteBuf content =
        response.hasData(ErrorData.class)
            ? encodeData(response.data(), response.dataFormatOrDefault())
            : ((ByteBuf) response.data()).retain();

    httpResponse.header("Access-Control-Allow-Origin", "*");
    return httpResponse.status(status).sendObject(content).then();
  }

  private Mono<Void> noContent(HttpServerResponse httpResponse) {
    httpResponse.header("Access-Control-Allow-Origin", "*");
    return httpResponse.status(NO_CONTENT).send();
  }

  private Mono<Void> ok(HttpServerResponse httpResponse, ServiceMessage response) {
    ByteBuf content =
        response.hasData(ByteBuf.class)
            ? ((ByteBuf) response.data()).retain()
            : encodeData(response.data(), response.dataFormatOrDefault());

    httpResponse.header("Access-Control-Allow-Origin", "*");
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

  private void enrichRequest(HttpHeaders requestHeaders, Builder builder) {
    Optional.ofNullable(requestHeaders.get(CLIENT_SEND_TIME))
        .ifPresent(value -> builder.header(CLIENT_SEND_TIME, value));

    Optional.ofNullable(requestHeaders.get(CLIENT_RECV_TIME))
        .ifPresent(value -> builder.header(CLIENT_RECV_TIME, value));

    Optional.ofNullable(requestHeaders.get(SERVICE_RECV_TIME))
        .ifPresent(value -> builder.header(SERVICE_RECV_TIME, value));

    Optional.ofNullable(requestHeaders.get(SERVICE_SEND_TIME))
        .ifPresent(value -> builder.header(SERVICE_SEND_TIME, value));
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
