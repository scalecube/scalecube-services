package io.scalecube.services.auth;

/** Session-based registry of authentication contexts. */
public interface AuthContextRegistry {

  /**
   * Adds authentication context to registry.
   *
   * @param sessionId session identifier
   * @param authContext authentication context
   * @return true of added, false otherwise
   */
  @SuppressWarnings("rawtypes")
  boolean addAuthContext(long sessionId, AuthContext authContext);

  /**
   * Retrieves authentication context of session.
   *
   * @param sessionId session identifier
   * @return authentication context
   */
  <C, P> AuthContext<C, P> getAuthContext(long sessionId);

  /**
   * Removes authentication context from registry.
   *
   * @param sessionId session identifier
   */
  void removeAuthContext(long sessionId);
}
