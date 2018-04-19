package io.scalecube.rsockets;

import io.rsocket.Payload;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;

public class MonoInvoker<REQ, RESP> extends ActionMethodInvoker<REQ, RESP> {

    public MonoInvoker(Class<REQ> reqType, Class<RESP> respType, PayloadCodec payloadCodec) {
        super(reqType, respType, CommunicationMode.MONO, payloadCodec);
    }

    @Override
    Publisher<RESP> invoke(ServiceReference sr, REQ request) {
        InetSocketAddress serverAddr = new InetSocketAddress(sr.host(), sr.port());
        client = RSocketFactory.connect()
                //TODO: handle errors
                .errorConsumer(Throwable::printStackTrace)
                .transport(TcpClientTransport.create(serverAddr))
                .start()
                .block();
        ServiceMessage serviceReq = request instanceof ServiceMessage
                ? (ServiceMessage) request
                : ServiceMessage.builder().qualifier(sr.namespace(), sr.action()).build();
        Mono<Payload> payloadMono = client.requestResponse(payloadCodec.encode(serviceReq));
        if (respType == ServiceMessage.class) {
            return payloadMono.map(payload -> payloadCodec.decode(payload)).cast(respType);
        } else {
            return payloadMono.map(payload -> payloadCodec.decode(payload, respType)).cast(respType);
        }
    }
}
