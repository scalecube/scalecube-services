package io.scalecube.services.examples.auth;

import io.scalecube.services.auth.MonoAuthUtil;
import io.scalecube.services.exceptions.ForbiddenException;
import reactor.core.publisher.Mono;

public class SecuredServiceByCompositeProfileImpl implements SecuredServiceByCompositeProfile {

  @Override
  public Mono<String> hello(String name) {
    return MonoAuthUtil.deferWithPrincipal(CompositeProfile.class)
        .flatMap(
            compositeProfile -> {
              final UserProfile userProfile = compositeProfile.userProfile();
              final ServiceEndpointProfile serviceEndpointProfile =
                  compositeProfile.serviceEndpointProfile();
              checkPermissions(userProfile);
              return Mono.just(
                  "Hello, name="
                      + name
                      + " (userProfile="
                      + userProfile
                      + ", serviceEndpointProfile="
                      + serviceEndpointProfile
                      + ")");
            });
  }

  private void checkPermissions(UserProfile user) {
    if (!user.role().equals("ADMIN")) {
      throw new ForbiddenException("Forbidden");
    }
  }
}
