package io.scalecube.services.gateway;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** So called "guess username" authentication. All preconfigured users can be authenticated. */
public class AuthRegistry {

  public static final String SESSION_ID = "SESSION_ID";

  /** Preconfigured userName-s that are allowed to be authenticated. */
  private final Set<String> allowedUsers;

  private ConcurrentMap<Long, String> loggedInUsers = new ConcurrentHashMap<>();

  public AuthRegistry(Set<String> allowedUsers) {
    this.allowedUsers = allowedUsers;
  }

  /**
   * Get session's auth data if exists.
   *
   * @param sessionId session id to get auth info for
   * @return auth info for given session if exists
   */
  public Optional<String> getAuth(long sessionId) {
    return Optional.ofNullable(loggedInUsers.get(sessionId));
  }

  /**
   * Add session with auth t registry.
   *
   * @param sessionId session id to add auth info for
   * @param auth auth info for given session id
   * @return auth info added for session id or empty if auth info is invalid
   */
  public Optional<String> addAuth(long sessionId, String auth) {
    if (allowedUsers.contains(auth)) {
      loggedInUsers.putIfAbsent(sessionId, auth);
      return Optional.of(auth);
    } else {
      System.err.println("User not in list of ALLOWED: " + auth);
    }
    return Optional.empty();
  }

  /**
   * Remove session from registry.
   *
   * @param sessionId session id to be removed from registry
   * @return true if session had auth info, false - otherwise
   */
  public String removeAuth(long sessionId) {
    return loggedInUsers.remove(sessionId);
  }
}
