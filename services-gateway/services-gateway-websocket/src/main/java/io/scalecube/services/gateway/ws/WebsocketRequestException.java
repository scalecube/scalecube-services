package io.scalecube.services.gateway.ws;

import io.scalecube.services.gateway.ReferenceCountUtil;
import java.util.Objects;
import java.util.Optional;

public class WebsocketRequestException extends RuntimeException {

  private final GatewayMessage request;

  private WebsocketRequestException(Throwable cause, GatewayMessage request) {
    super(Objects.requireNonNull(cause, "cause must be not null"));
    this.request = Objects.requireNonNull(request, "request message must be not null");
  }

  public static WebsocketRequestException newBadRequest(
      String errorMessage, GatewayMessage request) {
    return new WebsocketRequestException(
        new io.scalecube.services.exceptions.BadRequestException(errorMessage), request);
  }

  public GatewayMessage request() {
    return request;
  }

  public WebsocketRequestException releaseRequest() {
    Optional.ofNullable(request.data()).ifPresent(ReferenceCountUtil::safestRelease);
    return this;
  }
}
