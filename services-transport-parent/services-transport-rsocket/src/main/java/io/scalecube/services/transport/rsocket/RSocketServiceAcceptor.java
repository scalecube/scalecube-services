package io.scalecube.services.transport.rsocket;

import io.netty.buffer.ByteBufInputStream;
import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.AuthContext;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.methods.ServiceMethodInvoker;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.transport.api.DefaultHeadersCodec;
import io.scalecube.services.transport.api.ReferenceCountUtil;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

/**
 * RSocket service acceptor. Implementation of {@link SocketAcceptor}. See for details and supported
 * methods -- {@link AbstractRSocket0}.
 */
public class RSocketServiceAcceptor implements SocketAcceptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServiceAcceptor.class);

  private static final Consumer<Object> REQUEST_RELEASER = ReferenceCountUtil::safestRelease;

  private static final DefaultHeadersCodec DEFAULT_HEADERS_CODEC = new DefaultHeadersCodec();

  private final Authenticator authenticator;
  private final ServiceMessageCodec messageCodec;
  private final ServiceMethodRegistry methodRegistry;

  /**
   * Constructor.
   *
   * @param authenticator authenticator
   * @param messageCodec message codec
   * @param methodRegistry method registry
   */
  public RSocketServiceAcceptor(
      Authenticator authenticator,
      ServiceMessageCodec messageCodec,
      ServiceMethodRegistry methodRegistry) {
    this.authenticator = authenticator;
    this.messageCodec = messageCodec;
    this.methodRegistry = methodRegistry;
  }

  @Override
  public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket rsocket) {
    return authenticate(setup, rsocket)
        .doOnSubscribe(
            s -> {
              LOGGER.debug("Accepted rsocket: {}", rsocket);
              rsocket
                  .onClose()
                  .doFinally(sig -> LOGGER.debug("Closed rsocket: {}", rsocket))
                  .subscribe();
            })
        .onErrorResume(th -> Mono.empty())
        .flatMap(
            authData ->
                Mono.deferWithContext(context -> Mono.just(new AbstractRSocket0(context)))
                    .subscriberContext(context -> newConnectionContext(authData, context)));
  }

  private Mono<Map<String, String>> authenticate(ConnectionSetupPayload setup, RSocket rsocket) {
    return Mono.defer(
        () -> {
          if (authenticator == null) {
            return Mono.just(Collections.emptyMap());
          }

          Map<String, String> credentials = getCredentials(setup);
          if (credentials.isEmpty()) {
            return Mono.just(Collections.emptyMap());
          }

          return authenticator
              .authenticate(credentials)
              .doOnError(
                  th -> {
                    LOGGER.error(
                        "[authenticate] Exception occurred, cause: {}, rsocket: {}",
                        th.toString(),
                        rsocket);
                    rsocket.dispose();
                  })
              .doOnSuccess(
                  s ->
                      LOGGER.debug(
                          "[authenticate] authenticated successfully, rsocket: {}", rsocket));
        });
  }

  private Context newConnectionContext(Map<String, String> authData, Context context) {
    return authData.isEmpty() ? context : Context.of(AuthContext.class, new AuthContext(authData));
  }

  private static Map<String, String> getCredentials(ConnectionSetupPayload setup) {
    if (!setup.data().isReadable()) {
      return Collections.emptyMap();
    }
    try (InputStream stream = new ByteBufInputStream(setup.data())) {
      return DEFAULT_HEADERS_CODEC.decode(stream);
    } catch (IOException e) {
      throw Exceptions.propagate(e);
    }
  }

  private class AbstractRSocket0 extends AbstractRSocket {

    private final Context connectionContext;

    private AbstractRSocket0(Context connectionContext) {
      this.connectionContext = connectionContext;
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      return Mono.deferWithContext(context -> Mono.fromCallable(() -> toMessage(payload)))
          .doOnNext(this::validateRequest)
          .flatMap(
              message -> {
                ServiceMethodInvoker methodInvoker = methodRegistry.getInvoker(message.qualifier());
                validateMethodInvoker(methodInvoker, message);
                return methodInvoker.invokeOne(message).map(this::toPayload);
              })
          .subscriberContext(connectionContext);
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
      return Flux.deferWithContext(context -> Mono.fromCallable(() -> toMessage(payload)))
          .doOnNext(this::validateRequest)
          .flatMap(
              message -> {
                ServiceMethodInvoker methodInvoker = methodRegistry.getInvoker(message.qualifier());
                validateMethodInvoker(methodInvoker, message);
                return methodInvoker.invokeMany(message).map(this::toPayload);
              })
          .subscriberContext(connectionContext);
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
      return Flux.deferWithContext(context -> Flux.from(payloads).map(this::toMessage))
          .doOnNext(this::validateRequest)
          .switchOnFirst(
              (first, messages) -> {
                if (first.hasValue()) {
                  ServiceMessage message = first.get();
                  ServiceMethodInvoker methodInvoker =
                      methodRegistry.getInvoker(message.qualifier());
                  validateMethodInvoker(methodInvoker, message);
                  return methodInvoker.invokeBidirectional(messages);
                }
                return messages;
              })
          .map(this::toPayload)
          .subscriberContext(connectionContext);
    }

    private Payload toPayload(ServiceMessage response) {
      return messageCodec.encodeAndTransform(response, ByteBufPayload::create);
    }

    private ServiceMessage toMessage(Payload payload) {
      try {
        return messageCodec.decode(payload.sliceData().retain(), payload.sliceMetadata().retain());
      } finally {
        payload.release();
      }
    }

    private void validateRequest(ServiceMessage message) throws ServiceException {
      if (message.qualifier() == null) {
        applyRequestReleaser(message);
        LOGGER.error("[qualifier is null] Invocation failed for {}", message);
        throw new BadRequestException("Qualifier is null");
      }
    }

    private void validateMethodInvoker(ServiceMethodInvoker methodInvoker, ServiceMessage message) {
      if (methodInvoker == null) {
        applyRequestReleaser(message);
        LOGGER.error("[no service invoker found] Invocation failed for {}", message);
        throw new ServiceUnavailableException("No service invoker found");
      }
    }

    private void applyRequestReleaser(ServiceMessage request) {
      if (request.data() != null) {
        REQUEST_RELEASER.accept(request.data());
      }
    }
  }
}
