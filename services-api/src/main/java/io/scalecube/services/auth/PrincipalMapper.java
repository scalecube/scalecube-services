package io.scalecube.services.auth;

import io.scalecube.services.RequestContext;
import reactor.core.publisher.Mono;

/**
 * Functional interface for transforming existing {@link Principal} from {@link RequestContext} to
 * the new {@link Principal}. This interface allows to modify or replace a current principal from
 * {@link RequestContext}, allowing for dynamic adjustments to authentication and authorization
 * logic based on the request context.
 *
 * @see Principal
 * @see RequestContext
 */
@FunctionalInterface
public interface PrincipalMapper {

  /**
   * Maps current principal in the provided {@link RequestContext} to the new {@link Principal}.
   *
   * @param requestContext {@link RequestContext} containing the current principal
   * @return {@link Mono} emitting the new {@link Principal} after the transformation
   */
  Mono<Principal> map(RequestContext requestContext);
}
