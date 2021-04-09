package io.scalecube.services.security;

import io.scalecube.services.auth.Authenticator;
import java.util.Map;
import org.jctools.maps.NonBlockingHashMapLong;
import reactor.core.publisher.Mono;

/**
 * Implementation of {@link Authenticator} which works on top of existing authenticator and
 * internally maintains a map (registry) of auth data objects.
 *
 * @see #put(long, T)
 * @see #get(long)
 * @see #remove(long)
 * @see #containsKey(long)
 */
public final class RegistryBasedAuthenticator<T> implements Authenticator<T> {

  private final Authenticator<T> authenticator;

  private final Map<Long, T> registry = new NonBlockingHashMapLong<>();

  public RegistryBasedAuthenticator(Authenticator<T> authenticator) {
    this.authenticator = authenticator;
  }

  @Override
  public Mono<T> apply(Map<String, String> credentials) {
    return authenticator.apply(credentials);
  }

  public void put(long id, T authData) {
    registry.put(id, authData);
  }

  public T get(long id) {
    return registry.get(id);
  }

  public void remove(long id) {
    registry.remove(id);
  }

  public boolean containsKey(long id) {
    return registry.containsKey(id);
  }
}
