package io.scalecube.transport.utils.memoization;

public class MemoizerExecutionException extends RuntimeException {
  private static final long serialVersionUID = 4326445289311278879L;

  public MemoizerExecutionException(String message) {
    super(message);
  }

  public MemoizerExecutionException(String message, Throwable cause) {
    super(message, cause);
  }
}
