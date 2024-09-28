package io.scalecube.services.auth;

import java.util.Map;
import reactor.core.publisher.Mono;

public interface CredentialsSupplier {

  Mono<Map<String, String>> credentials(Map<String, String> tags);
}
