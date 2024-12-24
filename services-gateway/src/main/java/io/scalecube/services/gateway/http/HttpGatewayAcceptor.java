package io.scalecube.services.gateway.http;

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
import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.gateway.ReferenceCountUtil;
import io.scalecube.services.transport.api.DataCodec;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.function.BiFunction;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

public class HttpGatewayAcceptor
    implements BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> {

  private static final Logger LOGGER = System.getLogger(HttpGatewayAcceptor.class.getName());

  private static final String ERROR_NAMESPACE = "io.scalecube.services.error";

  private final ServiceCall serviceCall;
  private final ServiceProviderErrorMapper errorMapper;

  public HttpGatewayAcceptor(ServiceCall serviceCall, ServiceProviderErrorMapper errorMapper) {
    this.serviceCall = serviceCall;
    this.errorMapper = errorMapper;
  }

  @Override
  public Publisher<Void> apply(HttpServerRequest httpRequest, HttpServerResponse httpResponse) {
    LOGGER.log(
        Level.DEBUG,
        "Accepted request: {0}, headers: {}, params: {1}",
        httpRequest,
        httpRequest.requestHeaders(),
        httpRequest.params());

    if (httpRequest.method() != POST) {
      return methodNotAllowed(httpResponse);
    }

    return httpRequest
        .receive()
        .aggregate()
        .defaultIfEmpty(Unpooled.EMPTY_BUFFER)
        .map(ByteBuf::retain)
        .flatMap(content -> handleRequest(content, httpRequest, httpResponse))
        .onErrorResume(t -> error(httpResponse, errorMapper.toMessage(ERROR_NAMESPACE, t)));
  }

  private Mono<Void> handleRequest(
      ByteBuf content, HttpServerRequest httpRequest, HttpServerResponse httpResponse) {
    final var builder = ServiceMessage.builder();

    for (var httpHeader : httpRequest.requestHeaders()) {
      builder.header(httpHeader.getKey(), httpHeader.getValue());
    }

    final var qualifier = getQualifier(httpRequest);

    final var request =
        builder
            .header("httpMethod", httpRequest.method().name())
            .qualifier(qualifier)
            .data(content)
            .build();

    return serviceCall
        .requestOne(request)
        .switchIfEmpty(Mono.defer(() -> emptyMessage(qualifier)))
        .doOnError(th -> releaseRequestOnError(request))
        .flatMap(
            response ->
                response.isError() // check error
                    ? error(httpResponse, response)
                    : response.hasData() // check data
                        ? ok(httpResponse, response)
                        : noContent(httpResponse));
  }

  private static Mono<ServiceMessage> emptyMessage(final String qualifier) {
    return Mono.just(ServiceMessage.builder().qualifier(qualifier).build());
  }

  private static String getQualifier(HttpServerRequest httpRequest) {
    return httpRequest.uri().substring(1);
  }

  private static Publisher<Void> methodNotAllowed(HttpServerResponse httpResponse) {
    return httpResponse.addHeader(ALLOW, POST.name()).status(METHOD_NOT_ALLOWED).send();
  }

  private static Mono<Void> error(HttpServerResponse httpResponse, ServiceMessage response) {
    int code = response.errorType();
    HttpResponseStatus status = HttpResponseStatus.valueOf(code);

    ByteBuf content =
        response.hasData(ErrorData.class)
            ? encodeData(response.data(), response.dataFormatOrDefault())
            : ((ByteBuf) response.data());

    // send with publisher (defer buffer cleanup to netty)
    return httpResponse.status(status).send(Mono.just(content)).then();
  }

  private static Mono<Void> noContent(HttpServerResponse httpResponse) {
    return httpResponse.status(NO_CONTENT).send();
  }

  private static Mono<Void> ok(HttpServerResponse httpResponse, ServiceMessage response) {
    ByteBuf content =
        response.hasData(ByteBuf.class)
            ? ((ByteBuf) response.data())
            : encodeData(response.data(), response.dataFormatOrDefault());

    // send with publisher (defer buffer cleanup to netty)
    return httpResponse.status(OK).send(Mono.just(content)).then();
  }

  private static ByteBuf encodeData(Object data, String dataFormat) {
    ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();

    try {
      DataCodec.getInstance(dataFormat).encode(new ByteBufOutputStream(byteBuf), data);
    } catch (Throwable t) {
      ReferenceCountUtil.safestRelease(byteBuf);
      LOGGER.log(Level.ERROR, "Failed to encode data: {0}", data, t);
      return Unpooled.EMPTY_BUFFER;
    }

    return byteBuf;
  }

  private static void releaseRequestOnError(ServiceMessage request) {
    ReferenceCountUtil.safestRelease(request.data());
  }
}
