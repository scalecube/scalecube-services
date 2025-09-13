package io.scalecube.services.gateway.http;

import static io.netty.handler.codec.http.HttpHeaderNames.ALLOW;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.scalecube.services.api.ServiceMessage.HEADER_REQUEST_METHOD;
import static io.scalecube.services.api.ServiceMessage.HEADER_UPLOAD_FILENAME;
import static io.scalecube.services.gateway.ReferenceCountUtil.safestRelease;
import static io.scalecube.services.gateway.http.HttpGateway.SUPPORTED_METHODS;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.multipart.FileUpload;
import io.netty.handler.codec.http.multipart.HttpData;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.api.ServiceMessage.Builder;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.InternalServiceException;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.files.FileChannelFlux;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.StaticAddressRouter;
import io.scalecube.services.transport.api.DataCodec;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

public class HttpGatewayAcceptor
    implements BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpGatewayAcceptor.class);

  private static final String ERROR_NAMESPACE = "io.scalecube.services.error";
  private static final long MAX_SERVICE_MESSAGE_SIZE = 1024 * 1024;

  private final ServiceCall serviceCall;
  private final ServiceRegistry serviceRegistry;
  private final ServiceProviderErrorMapper errorMapper;
  private final HttpGatewayAuthenticator authenticator;

  public HttpGatewayAcceptor(
      ServiceCall serviceCall,
      ServiceRegistry serviceRegistry,
      ServiceProviderErrorMapper errorMapper,
      HttpGatewayAuthenticator authenticator) {
    this.serviceCall = serviceCall;
    this.serviceRegistry = serviceRegistry;
    this.errorMapper = errorMapper;
    this.authenticator = authenticator;
  }

  @Override
  public Publisher<Void> apply(HttpServerRequest httpRequest, HttpServerResponse httpResponse) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Accepted request: {}", httpRequest);
    }

    if (!SUPPORTED_METHODS.contains(httpRequest.method())) {
      return methodNotAllowed(httpResponse);
    }

    return Mono.defer(() -> authenticator.authenticate(httpRequest))
        .materialize()
        .flatMap(
            signal -> {
              if (signal.hasError()) {
                return error(
                    httpResponse, errorMapper.toMessage(ERROR_NAMESPACE, signal.getThrowable()));
              }

              final Map<String, String> principal =
                  signal.get() != null ? signal.get() : Collections.emptyMap();

              if (httpRequest.isMultipart()) {
                return handleFileUploadRequest(principal, httpRequest, httpResponse);
              } else {
                return handleServiceRequest(principal, httpRequest, httpResponse);
              }
            });
  }

  private Mono<Void> handleFileUploadRequest(
      Map<String, String> principal,
      HttpServerRequest httpRequest,
      HttpServerResponse httpResponse) {
    return httpRequest
        .receiveForm()
        .cast(FileUpload.class)
        .flatMap(
            fileUpload ->
                serviceCall
                    .requestBidirectional(
                        createFileFlux(fileUpload)
                            .map(
                                data ->
                                    toMessage(
                                        httpRequest,
                                        builder -> {
                                          final var filename = fileUpload.getFilename();
                                          builder
                                              .headers(principal)
                                              .header(HEADER_UPLOAD_FILENAME, filename)
                                              .data(data);
                                        })))
                    .last()
                    .flatMap(
                        response ->
                            response.isError() // check error
                                ? error(httpResponse, response)
                                : response.hasData() // check data
                                    ? ok(httpResponse, response)
                                    : noContent(httpResponse))
                    .doFinally(signalType -> fileUpload.delete()))
        .then()
        .onErrorResume(ex -> error(httpResponse, errorMapper.toMessage(ERROR_NAMESPACE, ex)));
  }

  private Mono<Void> handleServiceRequest(
      Map<String, String> principal,
      HttpServerRequest httpRequest,
      HttpServerResponse httpResponse) {
    return httpRequest
        .receive()
        .reduceWith(
            Unpooled::buffer,
            (reduce, byteBuf) -> {
              final var readableBytes = reduce.readableBytes();
              final var limit = MAX_SERVICE_MESSAGE_SIZE;
              if (readableBytes >= limit) {
                throw new BadRequestException(
                    "Service message is too large, size: " + readableBytes + ", limit: " + limit);
              }
              return reduce.writeBytes(byteBuf);
            })
        .defaultIfEmpty(Unpooled.EMPTY_BUFFER)
        .flatMap(
            data -> {
              final var message =
                  toMessage(httpRequest, builder -> builder.headers(principal).data(data));

              // Match and handle file-download request

              final var service = matchFileDownloadRequest(serviceRegistry.lookupService(message));
              if (service != null) {
                return handleFileDownloadRequest(service, message, httpResponse);
              }

              // Handle normal service request

              return serviceCall
                  .requestOne(message)
                  .switchIfEmpty(Mono.defer(() -> emptyMessage(message)))
                  .doOnError(th -> safestRelease(message.data()))
                  .flatMap(
                      response ->
                          response.isError() // check error
                              ? error(httpResponse, response)
                              : response.hasData() // check data
                                  ? ok(httpResponse, response)
                                  : noContent(httpResponse));
            })
        .onErrorResume(ex -> error(httpResponse, errorMapper.toMessage(ERROR_NAMESPACE, ex)));
  }

  private static ServiceMessage toMessage(
      HttpServerRequest httpRequest, Consumer<Builder> consumer) {
    final var builder = ServiceMessage.builder();

    // Copy http headers to service message

    for (var httpHeader : httpRequest.requestHeaders()) {
      builder.header(httpHeader.getKey(), httpHeader.getValue());
    }

    // Add http method to service message (used by REST services)

    builder
        .header(HEADER_REQUEST_METHOD, httpRequest.method().name())
        .qualifier(httpRequest.uri().substring(1));
    if (consumer != null) {
      consumer.accept(builder);
    }

    return builder.build();
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
      safestRelease(byteBuf);
      LOGGER.error("Failed to encode data: {}", data, t);
      return Unpooled.EMPTY_BUFFER;
    }

    return byteBuf;
  }

  private static ServiceReference matchFileDownloadRequest(List<ServiceReference> list) {
    if (list.size() != 1) {
      return null;
    }
    final var service = list.get(0);
    if ("application/file".equals(service.tags().get("Content-Type"))) {
      return service;
    } else {
      return null;
    }
  }

  private Mono<Void> handleFileDownloadRequest(
      ServiceReference service, ServiceMessage message, HttpServerResponse response) {
    return serviceCall
        .router(StaticAddressRouter.forService(service.address(), service.endpointName()).build())
        .requestMany(message)
        .switchOnFirst(
            (signal, flux) -> {
              if (signal.hasError()) {
                throw Exceptions.propagate(signal.getThrowable());
              }

              if (!signal.hasValue()) {
                throw new InternalServiceException("File stream is missing or invalid");
              }

              final var downloadMessage = signal.get();
              if (downloadMessage.isError()) {
                return error(response, downloadMessage);
              }

              downloadMessage.headers().forEach(response::header);

              return response
                  .send(
                      flux.skip(1)
                          .map(
                              sm -> {
                                if (sm.isError()) {
                                  throw new RuntimeException("File stream was interrupted");
                                }
                                return sm.data();
                              }))
                  .then();
            })
        .then();
  }

  private static Flux<byte[]> createFileFlux(HttpData httpData) {
    try {
      if (httpData.isInMemory()) {
        return Flux.just(httpData.get());
      } else {
        return FileChannelFlux.createFrom(httpData.getFile().toPath());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
