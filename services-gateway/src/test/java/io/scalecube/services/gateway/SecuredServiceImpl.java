package io.scalecube.services.gateway;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.ForbiddenException;
import java.util.Optional;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SecuredServiceImpl implements SecuredService {
  private static final Logger LOGGER = LoggerFactory.getLogger(SecuredServiceImpl.class);

  private static final String ALLOWED_USER = "VASYA_PUPKIN";

  private final AuthRegistry authRegistry;

  public SecuredServiceImpl(AuthRegistry authRegistry) {
    this.authRegistry = authRegistry;
  }

  @Override
  public Mono<String> createSession(ServiceMessage request) {
    String sessionId = request.header(AuthRegistry.SESSION_ID);
    if (sessionId == null) {
      return Mono.error(new BadRequestException("session Id is not present in request") {});
    }
    String req = request.data();
    Optional<String> authResult = authRegistry.addAuth(Long.parseLong(sessionId), req);
    if (authResult.isPresent()) {
      return Mono.just(req);
    } else {
      return Mono.error(new ForbiddenException("User not allowed to use this service: " + req));
    }
  }

  @Override
  public Mono<String> requestOne(String req) {
    return Mono.deferContextual(context -> Mono.just(context.get(Authenticator.AUTH_CONTEXT_KEY)))
        .doOnNext(this::checkPermissions)
        .cast(String.class)
        .flatMap(
            auth -> {
              LOGGER.info("User {} has accessed secured call", auth);
              return Mono.just(auth + "@" + req);
            });
  }

  @Override
  public Flux<String> requestN(Integer times) {
    return Mono.deferContextual(context -> Mono.just(context.get(Authenticator.AUTH_CONTEXT_KEY)))
        .doOnNext(this::checkPermissions)
        .cast(String.class)
        .flatMapMany(
            auth -> {
              if (times <= 0) {
                return Flux.empty();
              }
              return Flux.fromStream(IntStream.range(0, times).mapToObj(String::valueOf));
            });
  }

  private void checkPermissions(Object authData) {
    if (authData == null) {
      throw new ForbiddenException("Not allowed (authData is null)");
    }
    if (!authData.equals(ALLOWED_USER)) {
      throw new ForbiddenException("Not allowed (wrong user)");
    }
  }
}
