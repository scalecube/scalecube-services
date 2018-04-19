package io.scalecube.rsockets;

import io.rsocket.RSocket;
import io.scalecube.services.ServiceReference;
import org.reactivestreams.Publisher;

public abstract class ActionMethodInvoker<REQ, RESP> {

    protected final Class<REQ> reqType;
    protected final Class<RESP> respType;
    protected final CommunicationMode mode;

    protected PayloadCodec payloadCodec;
    protected RSocket client;

    protected ActionMethodInvoker(Class<REQ> reqType, Class<RESP> respType, CommunicationMode mode, PayloadCodec payloadCodec) {
        this.reqType = reqType;
        this.respType = respType;
        this.mode = mode;
        this.payloadCodec = payloadCodec;
    }

    abstract Publisher<RESP> invoke(ServiceReference serviceReference, REQ request);
}
