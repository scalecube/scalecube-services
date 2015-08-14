package io.servicefabric.transport.utils.memoization;

/**
 * The Interface Computable.
 *
 * @param <A> the generic type
 * @param <V> the value type
 */
public interface Computable<A, V> {

	/**
	 * Compute.
	 *
	 * @param arg the arg
	 * @return the v
	 * @throws Exception the exception
	 */
	V compute(A arg) throws Exception;

}
