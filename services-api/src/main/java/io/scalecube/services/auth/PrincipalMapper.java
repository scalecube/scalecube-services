package io.scalecube.services.auth;

import java.util.function.Function;

/**
 * Turns {@code authData} to concrete principal object.
 *
 * @see io.scalecube.services.ServiceInfo.Builder#principalMapper(PrincipalMapper)
 * @param <T> auth data type
 * @param <R> principal type
 */
@FunctionalInterface
public interface PrincipalMapper<T, R> extends Function<T, R> {}
