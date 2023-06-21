package io.scalecube.services.gateway;

import java.util.Map;

public interface GatewaySession {

  /**
   * Session id representation to be unique per client session.
   *
   * @return session id
   */
  long sessionId();

  /**
   * Returns headers associated with session.
   *
   * @return headers map
   */
  Map<String, String> headers();
}
