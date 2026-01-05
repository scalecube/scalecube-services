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
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
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
  private final HttpGatewayMessageHandler messageHandler;
  private final ServiceProviderErrorMapper errorMapper;
  private final HttpGatewayAuthenticator authenticator;

  public HttpGatewayAcceptor(
      ServiceCall serviceCall,
      ServiceRegistry serviceRegistry,
      HttpGatewayMessageHandler messageHandler,
      ServiceProviderErrorMapper errorMapper,
      HttpGatewayAuthenticator authenticator) {
    this.serviceCall = serviceCall;
    this.serviceRegistry = serviceRegistry;
    this.messageHandler = messageHandler;
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
        .map(
            data -> {
              if (!(data instanceof FileUpload file)) {
                throw new BadRequestException(
                    "Non file-upload part is not allowed, name=" + data.getName());
              }
              return file.retain();
            })
        .collectList()
        .flatMap(
            files -> {
              if (files.size() != 1) {
                return Mono.error(
                    new BadRequestException(
                        "Exactly one file-upload part is expected (received: "
                            + files.size()
                            + ")"));
              }

              final var fileUpload = files.get(0);
              final var filename = fileUpload.getFilename();

              return serviceCall
                  .requestBidirectional(
                      createFileFlux(fileUpload)
                          .map(
                              data ->
                                  toMessage(
                                      httpRequest,
                                      builder ->
                                          builder
                                              .headers(principal)
                                              .header(HEADER_UPLOAD_FILENAME, filename)
                                              .data(data))))
                  .last()
                  .flatMap(
                      response ->
                          response.isError() // check error
                              ? error(httpResponse, response)
                              : response.hasData() // check data
                                  ? ok(httpResponse, response)
                                  : noContent(httpResponse, response))
                  .doFinally(
                      signalType -> {
                        try {
                          fileUpload.delete();
                        } finally {
                          safestRelease(fileUpload);
                        }
                      });
            })
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
                    "Service message is too large (size: "
                        + readableBytes
                        + ", limit: "
                        + limit
                        + ")");
              }
              return reduce.writeBytes(byteBuf);
            })
        .defaultIfEmpty(Unpooled.EMPTY_BUFFER)
        .flatMap(
            data -> {
              final var message =
                  toMessage(httpRequest, builder -> builder.headers(principal).data(data));

              messageHandler.onRequest(httpRequest, data, message);

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
                                  : noContent(httpResponse, response));
            })
        .onErrorResume(ex -> error(httpResponse, errorMapper.toMessage(ERROR_NAMESPACE, ex)));
  }

  private static ServiceMessage toMessage(
      HttpServerRequest httpRequest, Consumer<Builder> consumer) {
    final var builder = ServiceMessage.builder();

    // Copy HTTP headers to service message

    for (var httpHeader : httpRequest.requestHeaders()) {
      builder.header("http.header." + httpHeader.getKey(), httpHeader.getValue());
    }

    // Copy HTTP query params to service message

    final var uri = httpRequest.uri();
    final var queryParams = matchQueryParams(uri);
    queryParams.forEach((param, value) -> builder.header("http.query." + param, value));

    // Add HTTP method to service message (used by REST services)

    builder
        .header("http.method", httpRequest.method().name())
        .header(HEADER_REQUEST_METHOD, httpRequest.method().name())
        .qualifier(stripQueryParams(uri.substring(1)));

    if (consumer != null) {
      consumer.accept(builder);
    }

    return builder.build();
  }

  private static Map<String, String> matchQueryParams(String uri) {
    final var index = uri.indexOf('?');
    if (index < 0 || index == uri.length() - 1) {
      return Collections.emptyMap(); // no query params
    }
    return Arrays.stream(uri.substring(index + 1).split("&"))
        .map(s -> s.split("=", 2))
        .filter(parts -> parts.length == 2)
        .collect(
            Collectors.toMap(
                parts -> URLDecoder.decode(parts[0], StandardCharsets.UTF_8),
                parts -> URLDecoder.decode(parts[1], StandardCharsets.UTF_8)));
  }

  private static String stripQueryParams(String uri) {
    final var index = uri.indexOf('?');
    if (index < 0) {
      return uri; // no query params
    }
    return uri.substring(0, index);
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

  private Mono<Void> error(HttpServerResponse httpResponse, ServiceMessage response) {
    int code = response.errorType();
    HttpResponseStatus status = HttpResponseStatus.valueOf(code);

    ByteBuf content =
        response.hasData(ErrorData.class)
            ? encodeData(response.data(), response.dataFormatOrDefault())
            : ((ByteBuf) response.data());

    messageHandler.onError(httpResponse, content, response);

    // Send with publisher (defer buffer cleanup to netty)

    return httpResponse.status(status).send(Mono.just(content)).then();
  }

  private Mono<Void> noContent(HttpServerResponse httpResponse, ServiceMessage response) {
    messageHandler.onResponse(httpResponse, Unpooled.EMPTY_BUFFER, response);
    return httpResponse.status(NO_CONTENT).send();
  }

  private Mono<Void> ok(HttpServerResponse httpResponse, ServiceMessage response) {
    ByteBuf content =
        response.hasData(ByteBuf.class)
            ? ((ByteBuf) response.data())
            : encodeData(response.data(), response.dataFormatOrDefault());

    messageHandler.onResponse(httpResponse, content, response);

    // Send with publisher (defer buffer cleanup to netty)

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
