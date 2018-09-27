package io.scalecube.gateway.websocket;

import io.scalecube.gateway.ReferenceCountUtil;
import io.scalecube.gateway.websocket.message.GatewayMessage;
import java.util.Objects;
import java.util.Optional;

public class WebsocketException extends RuntimeException {

  private final GatewayMessage request;

  private WebsocketException(Throwable cause, GatewayMessage request) {
    super(Objects.requireNonNull(cause, "cause must be not null"));
    this.request = Objects.requireNonNull(request, "request message must be not null");
  }

  public static WebsocketException newBadRequest(String errorMessage, GatewayMessage request) {
    return new WebsocketException(
        new io.scalecube.services.exceptions.BadRequestException(errorMessage), request);
  }

  public GatewayMessage request() {
    return request;
  }

  public WebsocketException releaseRequest() {
    Optional.ofNullable(request.data()).ifPresent(ReferenceCountUtil::safestRelease);
    return this;
  }
}
