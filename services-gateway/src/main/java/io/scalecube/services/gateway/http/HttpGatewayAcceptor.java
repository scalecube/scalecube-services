package io.scalecube.services.gateway.http;

import static io.netty.handler.codec.http.HttpHeaderNames.ALLOW;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.scalecube.services.api.ServiceMessage.HEADER_REQUEST_METHOD;
import static io.scalecube.services.gateway.http.HttpGateway.SUPPORTED_METHODS;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.DynamicQualifier;
import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.gateway.ReferenceCountUtil;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.StaticAddressRouter;
import io.scalecube.services.transport.api.DataCodec;
import java.util.List;
import java.util.function.BiFunction;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

public class HttpGatewayAcceptor
    implements BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpGatewayAcceptor.class);

  private static final String ERROR_NAMESPACE = "io.scalecube.services.error";

  private final ServiceCall serviceCall;
  private final ServiceRegistry serviceRegistry;
  private final ServiceProviderErrorMapper errorMapper;

  public HttpGatewayAcceptor(
      ServiceCall serviceCall,
      ServiceRegistry serviceRegistry,
      ServiceProviderErrorMapper errorMapper) {
    this.serviceCall = serviceCall;
    this.serviceRegistry = serviceRegistry;
    this.errorMapper = errorMapper;
  }

  @Override
  public Publisher<Void> apply(HttpServerRequest httpRequest, HttpServerResponse httpResponse) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Accepted request: {}, headers: {}, params: {}",
          httpRequest,
          httpRequest.requestHeaders(),
          httpRequest.params());
    }

    if (!SUPPORTED_METHODS.contains(httpRequest.method())) {
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
    final var message = toMessage(httpRequest, content);

    // Match and handle file request

    final var serviceReference = matchFileRequest(serviceRegistry.lookupService(message));
    if (serviceReference != null) {
      return handleFileRequest(serviceReference, message, httpResponse);
    }

    // Handle normal service request

    return serviceCall
        .requestOne(message)
        .switchIfEmpty(Mono.defer(() -> emptyMessage(message)))
        .doOnError(th -> releaseRequestOnError(message))
        .flatMap(
            response ->
                response.isError() // check error
                    ? error(httpResponse, response)
                    : response.hasData() // check data
                        ? ok(httpResponse, response)
                        : noContent(httpResponse));
  }

  private static ServiceMessage toMessage(HttpServerRequest httpRequest, ByteBuf content) {
    final var builder = ServiceMessage.builder();

    // Copy http headers to service message

    for (var httpHeader : httpRequest.requestHeaders()) {
      builder.header(httpHeader.getKey(), httpHeader.getValue());
    }

    // Add http method to service message (used by REST services)

    return builder
        .header(HEADER_REQUEST_METHOD, httpRequest.method().name())
        .qualifier(httpRequest.uri().substring(1))
        .data(content)
        .build();
  }

  private static Mono<ServiceMessage> emptyMessage(ServiceMessage message) {
    return Mono.just(ServiceMessage.builder().qualifier(message.qualifier()).build());
  }

  private static Publisher<Void> methodNotAllowed(HttpServerResponse httpResponse) {
    return httpResponse
        .addHeader(
            ALLOW,
            String.join(
                ", ", SUPPORTED_METHODS.stream().map(HttpMethod::name).toArray(String[]::new)))
        .status(METHOD_NOT_ALLOWED)
        .send();
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
      LOGGER.error("Failed to encode data: {}", data, t);
      return Unpooled.EMPTY_BUFFER;
    }

    return byteBuf;
  }

  private static void releaseRequestOnError(ServiceMessage request) {
    ReferenceCountUtil.safestRelease(request.data());
  }

  private static ServiceReference matchFileRequest(List<ServiceReference> list) {
    if (list.size() != 1) {
      return null;
    }
    final var sr = list.get(0);
    if ("application/file".equals(sr.tags().get("Content-Type"))) {
      return sr;
    } else {
      return null;
    }
  }

  private Mono<Void> handleFileRequest(
      ServiceReference service, ServiceMessage message, HttpServerResponse response) {
    return serviceCall
        .router(
            StaticAddressRouter.from(service.address()).serviceName(service.endpointId()).build())
        .requestMany(message)
        .switchOnFirst(
            (signal, flux) -> {
              final var qualifier = message.qualifier();
              final var map =
                  DynamicQualifier.from("v1/endpoints/:endpointId/files/:name")
                      .matchQualifier(qualifier);
              if (map == null) {
                throw new RuntimeException("Wrong qualifier: " + qualifier);
              }

              final var fileName = map.get("name");
              final var statusCode = toStatusCode(signal);

              if (statusCode != HttpResponseStatus.OK.code()) {
                return response
                    .status(statusCode)
                    .sendString(Mono.just(errorMessage(statusCode, fileName)))
                    .then();
              }

              final Flux<ByteBuf> responseFlux =
                  flux.map(
                      sm -> {
                        if (sm.isError()) {
                          throw new RuntimeException("File stream was interrupted");
                        }
                        return sm.data();
                      });

              return response
                  .header("Content-Type", "application/octet-stream")
                  .header("Content-Disposition", "attachment; filename=" + fileName)
                  .send(responseFlux)
                  .then();
            })
        .then();
  }

  private static int toStatusCode(Signal<? extends ServiceMessage> signal) {
    if (signal.hasError()) {
      return toStatusCode(signal.getThrowable());
    }

    if (!signal.hasValue()) {
      return HttpResponseStatus.NO_CONTENT.code();
    }

    return toStatusCode(signal.get());
  }

  private static int toStatusCode(Throwable throwable) {
    if (throwable instanceof ServiceException e) {
      return e.errorCode();
    } else {
      return HttpResponseStatus.INTERNAL_SERVER_ERROR.code();
    }
  }

  private static int toStatusCode(ServiceMessage serviceMessage) {
    if (serviceMessage == null || !serviceMessage.hasData()) {
      return HttpResponseStatus.NO_CONTENT.code();
    }

    if (serviceMessage.isError()) {
      return HttpResponseStatus.INTERNAL_SERVER_ERROR.code();
    }

    return HttpResponseStatus.OK.code();
  }

  private static String errorMessage(int statusCode, String fileName) {
    if (statusCode == 500) {
      return "File not found: " + fileName;
    } else {
      return HttpResponseStatus.valueOf(statusCode).reasonPhrase();
    }
  }
}
