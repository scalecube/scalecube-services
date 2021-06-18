package io.scalecube.services.exceptions;

import java.util.regex.Pattern;

public class ConnectionClosedException extends InternalServiceException {

  private static final Pattern GENERIC_CONNECTION_CLOSED =
      Pattern.compile(
          "^.*(?:connection.*(?:reset|closed|abort|broken)|broken.*pipe).*$",
          Pattern.CASE_INSENSITIVE);

  public ConnectionClosedException() {
    super("Connection closed");
  }

  public ConnectionClosedException(Throwable cause) {
    super(cause);
  }

  public ConnectionClosedException(String message) {
    super(message);
  }

  /**
   * Returns {@code true} if connection has been aborted on a tcp level by verifying error message
   * and matching it against predefined pattern.
   *
   * @param th error
   * @return {@code true} if connection has been aborted on a tcp level
   */
  public static boolean isConnectionClosed(Throwable th) {
    if (th instanceof ConnectionClosedException) {
      return true;
    }

    final String message = th != null ? th.getMessage() : null;

    return message != null && GENERIC_CONNECTION_CLOSED.matcher(message).matches();
  }
}
