package io.scalecube.services.gateway.http;

import java.util.Collections;
import java.util.Map;
import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServerRequest;

public interface HttpGatewayAuthenticator {

  HttpGatewayAuthenticator DEFAULT_INSTANCE = new HttpGatewayAuthenticator() {};

  default Mono<Map<String, String>> authenticate(HttpServerRequest request) {
    return Mono.just(Collections.emptyMap());
  }
}
