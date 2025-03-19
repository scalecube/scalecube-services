package io.scalecube.services.auth;

import io.scalecube.services.RequestContext;
import reactor.core.publisher.Mono;

@FunctionalInterface
public interface PrincipalMapper {

  Mono<Principal> map(RequestContext requestContext);
}
