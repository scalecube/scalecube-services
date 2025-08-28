package io.scalecube.services.methods;

import io.scalecube.services.CommunicationMode;
import io.scalecube.services.RequestContext;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Principal;
import io.scalecube.services.auth.PrincipalMapper;
import io.scalecube.services.exceptions.ForbiddenException;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

public class ServiceMethodInvoker {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceMethodInvoker.class);

  private final Method method;
  private final Object service;
  private final MethodInfo methodInfo;
  private final ServiceProviderErrorMapper errorMapper;
  private final ServiceMessageDataDecoder dataDecoder;
  private final PrincipalMapper principalMapper;
  private final Logger logger;

  public ServiceMethodInvoker(
      Method method,
      Object service,
      MethodInfo methodInfo,
      ServiceProviderErrorMapper errorMapper,
      ServiceMessageDataDecoder dataDecoder,
      PrincipalMapper principalMapper,
      Logger logger) {
    this.method = Objects.requireNonNull(method, "method");
    this.service = Objects.requireNonNull(service, "service");
    this.methodInfo = Objects.requireNonNull(methodInfo, "methodInfo");
    this.errorMapper = Objects.requireNonNull(errorMapper, "errorMapper");
    this.dataDecoder = Objects.requireNonNull(dataDecoder, "dataDecoder");
    this.principalMapper = principalMapper;
    this.logger = logger;
  }

  /**
   * Invokes service method with single response.
   *
   * @param message request service message
   * @return mono of service message
   */
  public Mono<ServiceMessage> invokeOne(ServiceMessage message) {
    return RequestContext.deferContextual()
        .flatMap(
            context -> {
              final var request = toRequest(message);
              return mapPrincipal(context)
                  .flatMap(
                      principal ->
                          requestContext()
                              .then(Mono.defer(() -> Mono.from(invokeRequest(request))))
                              .contextWrite(enhanceRequestContext(context, request, principal)))
                  .doOnSuccess(
                      response -> {
                        if (logger != null && logger.isDebugEnabled()) {
                          logger.debug(
                              "[{}] request: {}, response: {}",
                              message.qualifier(),
                              request,
                              response);
                        }
                      })
                  .doOnError(
                      ex -> {
                        if (logger != null) {
                          logger.error("[{}][error] request: {}", message.qualifier(), request, ex);
                        }
                      });
            })
        .map(response -> toResponse(response, message.qualifier(), message.dataFormat()))
        .onErrorResume(ex -> Mono.just(errorMapper.toMessage(message.qualifier(), ex)))
        .subscribeOn(methodInfo.scheduler());
  }

  /**
   * Invokes service method with message stream response.
   *
   * @param message request service message
   * @return flux of service messages
   */
  public Flux<ServiceMessage> invokeMany(ServiceMessage message) {
    if (methodInfo.communicationMode() == CommunicationMode.REQUEST_RESPONSE) {
      return Flux.from(invokeOne(message));
    }
    return RequestContext.deferContextual()
        .flatMapMany(
            context -> {
              final var request = toRequest(message);
              return mapPrincipal(context)
                  .flatMapMany(
                      principal ->
                          requestContext()
                              .thenMany(Flux.defer(() -> Flux.from(invokeRequest(request))))
                              .contextWrite(enhanceRequestContext(context, request, principal)))
                  .doOnSubscribe(
                      s -> {
                        if (logger != null && logger.isDebugEnabled()) {
                          logger.debug("[{}][subscribe] request: {}", message.qualifier(), request);
                        }
                      })
                  .doOnComplete(
                      () -> {
                        if (logger != null && logger.isDebugEnabled()) {
                          logger.debug("[{}][complete] request: {}", message.qualifier(), request);
                        }
                      })
                  .doOnError(
                      ex -> {
                        if (logger != null) {
                          logger.error("[{}][error] request: {}", message.qualifier(), request, ex);
                        }
                      });
            })
        .map(response -> toResponse(response, message.qualifier(), message.dataFormat()))
        .onErrorResume(ex -> Flux.just(errorMapper.toMessage(message.qualifier(), ex)))
        .subscribeOn(methodInfo.scheduler());
  }

  /**
   * Invokes service method with bidirectional communication.
   *
   * @param publisher request service message
   * @return flux of service messages
   */
  public Flux<ServiceMessage> invokeBidirectional(Publisher<ServiceMessage> publisher) {
    return Flux.from(publisher)
        .switchOnFirst(
            (first, messages) -> {
              final var message = first.get();
              final var request = toRequest(message);
              final var qualifier = message.qualifier();
              final var dataFormat = message.dataFormat();

              return messages
                  .map(this::toRequest)
                  .transform(this::invokeRequest)
                  .doOnSubscribe(
                      s -> {
                        if (logger != null && logger.isDebugEnabled()) {
                          logger.debug("[{}][subscribe] request: {}", message.qualifier(), request);
                        }
                      })
                  .doOnComplete(
                      () -> {
                        if (logger != null && logger.isDebugEnabled()) {
                          logger.debug("[{}][complete] request: {}", message.qualifier(), request);
                        }
                      })
                  .doOnError(
                      ex -> {
                        if (logger != null) {
                          logger.error("[{}][error] request: {}", message.qualifier(), request, ex);
                        }
                      })
                  .map(response -> toResponse(response, qualifier, dataFormat))
                  .onErrorResume(ex -> Flux.just(errorMapper.toMessage(qualifier, ex)))
                  .subscribeOn(methodInfo.scheduler());
            });
  }

  private Mono<RequestContext> requestContext() {
    final Mono<RequestContext> contextMono;
    final var secured = methodInfo.secured();

    if (secured != null && secured.deferSecured()) {
      contextMono = RequestContext.deferSecured();
    } else {
      contextMono = RequestContext.deferContextual();
    }

    return contextMono;
  }

  private Publisher<?> invokeRequest(Object request) {
    Publisher<?> result = null;
    Throwable throwable = null;
    try {
      if (methodInfo.parameterCount() == 0) {
        result = (Publisher<?>) method.invoke(service);
      } else {
        Object[] arguments = prepareArguments(request);
        result = (Publisher<?>) method.invoke(service, arguments);
      }
      if (result == null) {
        result = Mono.empty();
      }
    } catch (InvocationTargetException ex) {
      throwable = ex.getCause();
    } catch (Throwable ex) {
      throwable = ex;
    }
    return throwable != null ? Mono.error(throwable) : result;
  }

  private Object[] prepareArguments(Object request) {
    Object[] arguments = new Object[methodInfo.parameterCount()];
    if (methodInfo.requestType() != Void.TYPE) {
      arguments[0] = request;
    }
    return arguments;
  }

  private Context enhanceRequestContext(
      RequestContext context, Object request, Principal principal) {
    final var dynamicQualifier = methodInfo.dynamicQualifier();

    Map<String, String> pathVars = null;
    if (dynamicQualifier != null) {
      pathVars = dynamicQualifier.matchQualifier(context.requestQualifier());
    }

    return new RequestContext(context)
        .request(request)
        .principal(principal)
        .pathVars(pathVars)
        .methodInfo(methodInfo);
  }

  private Object toRequest(ServiceMessage message) {
    final var request = dataDecoder.apply(message, methodInfo.requestType());
    return methodInfo.isRequestTypeServiceMessage() ? request : request.data();
  }

  private static ServiceMessage toResponse(Object response, String qualifier, String dataFormat) {
    if (response instanceof ServiceMessage message) {
      final var builder = ServiceMessage.from(message).qualifier(qualifier);
      return dataFormat != null && !dataFormat.equals(message.dataFormat())
          ? builder.dataFormat(dataFormat).build()
          : builder.build();
    }
    return ServiceMessage.builder()
        .qualifier(qualifier)
        .data(response)
        .dataFormatIfAbsent(dataFormat)
        .build();
  }

  public Object service() {
    return service;
  }

  public MethodInfo methodInfo() {
    return methodInfo;
  }

  public PrincipalMapper principalMapper() {
    return principalMapper;
  }

  private Mono<Principal> mapPrincipal(RequestContext context) {
    if (methodInfo.secured() == null) {
      return Mono.just(context.principal());
    }

    if (principalMapper == null) {
      if (context.hasPrincipal()) {
        return Mono.just(context.principal());
      } else {
        LOGGER.warn(
            "Insufficient permissions for secured method ({}) -- "
                + "request context ({}) does not have principal"
                + "and principalMapper is also not set",
            methodInfo,
            context);
        throw new ForbiddenException("Insufficient permissions");
      }
    }

    return Mono.defer(() -> principalMapper.map(context))
        .switchIfEmpty(Mono.just(context.principal()));
  }
}
