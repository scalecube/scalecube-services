package io.scalecube.transport;

/**
 * @author Anton Kharenko
 */
public final class NetworkEmulatorException extends RuntimeException {

  public NetworkEmulatorException(String message) {
    super(message);
  }

  /**
   * No need for stack trace since those exceptions are not really an exceptions, but checked error conditions.
   */
  @Override
  public synchronized Throwable fillInStackTrace() {
    return this;
  }
}
