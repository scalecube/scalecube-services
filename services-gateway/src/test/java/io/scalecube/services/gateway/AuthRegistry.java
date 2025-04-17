package io.scalecube.services.gateway;

import io.scalecube.services.auth.Principal;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** So called "guess username" authentication. All preconfigured users can be authenticated. */
public class AuthRegistry {

  public static final String SESSION_ID = "SESSION_ID";

  private final Set<String> allowedUsers;
  private final ConcurrentMap<Long, Principal> loggedInUsers = new ConcurrentHashMap<>();

  /**
   * Constructor.
   *
   * @param allowedUsers preconfigured usernames that are allowed to be authenticated.
   */
  public AuthRegistry(Set<String> allowedUsers) {
    this.allowedUsers = allowedUsers;
  }

  /**
   * Get session auth data if exists.
   *
   * @param sessionId sessionId
   * @return principal by sessionId
   */
  public Principal getAuth(long sessionId) {
    return loggedInUsers.get(sessionId);
  }

  /**
   * Add session auth data.
   *
   * @param sessionId sessionId
   * @param username username
   */
  public boolean addAuth(long sessionId, String username) {
    if (allowedUsers.contains(username)) {
      loggedInUsers.putIfAbsent(sessionId, new AllowedUser(username));
      return true;
    }
    return false;
  }

  /**
   * Remove session from registry.
   *
   * @param sessionId sessionId
   * @return principal, or null if not exists
   */
  public Principal removeAuth(long sessionId) {
    return loggedInUsers.remove(sessionId);
  }

  public record AllowedUser(String username) implements Principal {}
}
