package io.scalecube.streams.exceptions;

public class StreamException extends RuntimeException {

  final int code;
  final String message;

  public StreamException(int code, String message) {
    super("code: " + code + ", message: " + message);
    this.code = code;
    this.message = message;
  }

  @Override
  public Throwable fillInStackTrace() {
    return this;
  }

  @Override
  public void setStackTrace(StackTraceElement[] stackTrace) {
    // NOP
  }

  @Override
  public String toString() {
    return "code: " + code + ", message: " + message;
  }
}
