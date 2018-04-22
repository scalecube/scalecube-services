package io.scalecube.services.transport.api;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codecs.api.ServiceMessageDataCodec;

import org.reactivestreams.Publisher;

public interface ServiceMethodDispatcher<REQ> {

  /**
   * REQ is expected to be ServiceMessage | Publisher<ServiceMessage>
   * @param request
   * @return
   */
  Publisher<ServiceMessage> invoke(REQ request);

  String methodName();

  ServiceMessageDataCodec getCodec();
}
