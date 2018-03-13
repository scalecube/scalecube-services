package io.scalecube.services;

import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;

class StringUnaryOperatorImpl implements StringUnaryOperator {

  private final UnaryOperator<String> delegate;

  protected StringUnaryOperatorImpl(UnaryOperator<String> delegate) {
    this.delegate = delegate;
  }

  @Override
  public CompletableFuture<String> apply(String t) {
    return CompletableFuture.completedFuture(t).thenApplyAsync(delegate);
  };

  @Override
  public String toString() {
    return delegate.toString();
  }
};
