package io.scalecube.services.auth;

import io.scalecube.services.ServiceReference;
import java.util.Map;
import java.util.function.Function;
import reactor.core.publisher.Mono;

/**
 * Returns credentials for the given {@link ServiceReference}. Credentials are being returned in
 * most generic form which is {@code Map<String, String>}.
 */
@FunctionalInterface
public interface CredentialsSupplier
    extends Function<ServiceReference, Mono<Map<String, String>>> {}
