package io.scalecube.services.security;

import io.scalecube.services.auth.Authenticator;
import java.util.Map;
import org.jctools.maps.NonBlockingHashMapLong;
import reactor.core.publisher.Mono;

/**
 * Implementation of {@link Authenticator} which works on top of existing {@code authenticator}.
 * Internally maintains a map of service claims where key is some id (of type {@code long}) and
 * value is {@link ServiceClaims} object.
 *
 * @see #saveAuthData(long, ServiceClaims)
 * @see #getAuthData(long)
 * @see #removeAuthData(long)
 */
public final class CompositeAuthenticator implements Authenticator<ServiceClaims> {

  private final Authenticator<ServiceClaims> authenticator;

  private final Map<Long, ServiceClaims> registry = new NonBlockingHashMapLong<>();

  public CompositeAuthenticator(Authenticator<ServiceClaims> authenticator) {
    this.authenticator = authenticator;
  }

  @Override
  public Mono<ServiceClaims> apply(Map<String, String> credentials) {
    return authenticator.apply(credentials);
  }

  public void saveAuthData(long id, ServiceClaims serviceClaims) {
    registry.put(id, serviceClaims);
  }

  public ServiceClaims getAuthData(long id) {
    return registry.get(id);
  }

  public void removeAuthData(long id) {
    registry.remove(id);
  }

  public boolean containsAuthData(long id) {
    return registry.containsKey(id);
  }
}
