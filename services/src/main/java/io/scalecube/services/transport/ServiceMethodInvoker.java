package io.scalecube.services.transport;

import io.scalecube.services.api.ServiceMessage;

import org.reactivestreams.Publisher;

public interface ServiceMethodInvoker<REQ> {

  Publisher<ServiceMessage> invoke(REQ request);

}
