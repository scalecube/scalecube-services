package io.scalecube.services.gateway.websocket;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.gateway.ReferenceCountUtil;

public class WebsocketContextException extends RuntimeException {

  private final ServiceMessage request;
  private final ServiceMessage response;

  private WebsocketContextException(
      Throwable cause, ServiceMessage request, ServiceMessage response) {
    super(cause);
    this.request = request;
    this.response = response;
  }

  public static WebsocketContextException badRequest(String errorMessage, ServiceMessage request) {
    return new WebsocketContextException(
        new io.scalecube.services.exceptions.BadRequestException(errorMessage), request, null);
  }

  public ServiceMessage request() {
    return request;
  }

  public ServiceMessage response() {
    return response;
  }

  /**
   * Releases request data if any.
   *
   * @return self
   */
  public WebsocketContextException releaseRequest() {
    if (request != null) {
      ReferenceCountUtil.safestRelease(request.data());
    }
    return this;
  }
}
