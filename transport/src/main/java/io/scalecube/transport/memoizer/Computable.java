package io.scalecube.transport.memoizer;

/**
 * The Interface Computable.
 *
 * @param <A> the type of argument
 * @param <V> the type of result value
 */
public interface Computable<A, V> {

  /**
   * Compute value based on given argument.
   *
   * @param arg the argument
   * @return The computed value
   * @throws Exception the exception
   */
  V compute(A arg) throws Exception;

}
