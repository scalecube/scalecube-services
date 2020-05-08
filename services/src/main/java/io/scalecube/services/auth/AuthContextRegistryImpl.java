package io.scalecube.services.auth;

import java.util.Objects;
import org.jctools.maps.NonBlockingHashMapLong;

@SuppressWarnings("rawtypes")
public class AuthContextRegistryImpl implements AuthContextRegistry {

  private final NonBlockingHashMapLong<AuthContext> registry = new NonBlockingHashMapLong<>();

  @Override
  public boolean addAuthContext(long sessionId, AuthContext authContext) {
    Objects.requireNonNull(authContext, "authContext");
    return registry.putIfAbsent(sessionId, authContext) == null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <C, P> AuthContext<C, P> getAuthContext(long sessionId) {
    return registry.get(sessionId);
  }

  @Override
  public void removeAuthContext(long sessionId) {
    registry.remove(sessionId);
  }
}
