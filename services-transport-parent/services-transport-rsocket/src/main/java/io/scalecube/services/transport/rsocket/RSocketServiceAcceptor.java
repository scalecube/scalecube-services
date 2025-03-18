package io.scalecube.services.transport.rsocket;

import static io.scalecube.services.auth.Principal.NULL_PRINCIPAL;

import io.netty.buffer.ByteBuf;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.auth.Principal;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class RSocketServiceAcceptor implements SocketAcceptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServiceAcceptor.class);

  private final HeadersCodec headersCodec;
  private final Collection<DataCodec> dataCodecs;
  private final Authenticator authenticator;
  private final ServiceRegistry serviceRegistry;

  public RSocketServiceAcceptor(
      HeadersCodec headersCodec,
      Collection<DataCodec> dataCodecs,
      Authenticator authenticator,
      ServiceRegistry serviceRegistry) {
    this.headersCodec = headersCodec;
    this.dataCodecs = dataCodecs;
    this.authenticator = authenticator;
    this.serviceRegistry = serviceRegistry;
  }

  @Override
  public Mono<RSocket> accept(ConnectionSetupPayload setupPayload, RSocket rsocket) {
    return Mono.defer(() -> authenticate(setupPayload.data())).map(this::newRSocket);
  }

  private Mono<Principal> authenticate(ByteBuf connectionSetup) {
    if (authenticator == null || !connectionSetup.isReadable()) {
      return Mono.just(NULL_PRINCIPAL);
    }

    final var credentials = new byte[connectionSetup.readableBytes()];
    connectionSetup.getBytes(connectionSetup.readerIndex(), credentials);

    return Mono.defer(() -> authenticator.authenticate(credentials))
        .switchIfEmpty(Mono.just(NULL_PRINCIPAL))
        .doOnSuccess(principal -> LOGGER.debug("Authenticated successfully: {}", principal))
        .doOnError(ex -> LOGGER.error("Authentication failed", ex))
        .onErrorMap(ex -> new UnauthorizedException("Authentication failed"));
  }

  private RSocket newRSocket(Principal principal) {
    return new RSocketImpl(
        principal, new ServiceMessageCodec(headersCodec, dataCodecs), serviceRegistry);
  }
}
