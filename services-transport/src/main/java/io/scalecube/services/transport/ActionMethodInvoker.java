package io.scalecube.services.transport;

import org.reactivestreams.Publisher;

public interface ActionMethodInvoker<REQ,RESP> {

  Publisher<RESP> invoke(REQ request);

}
