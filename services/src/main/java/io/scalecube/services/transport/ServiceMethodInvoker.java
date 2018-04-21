package io.scalecube.services.transport;

import io.scalecube.services.api.ServiceMessage;

import org.reactivestreams.Publisher;

public interface ServiceMethodInvoker<REQ> {

  /**
   * REQ is expected to be ServiceMessage | Publisher<ServiceMessage>
   * @param request
   * @return
   */
  Publisher<ServiceMessage> invoke(REQ request);

  String methodName();

}
