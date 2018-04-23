package io.scalecube.rsockets;

import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codecs.api.ServiceMessageCodec;

import io.rsocket.Payload;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Mono;

public class MonoInvoker<REQ, RESP> extends ActionMethodInvoker<REQ, RESP> {

    public MonoInvoker(Class<REQ> reqType, Class<RESP> respType, ServiceMessageCodec payloadCodec) {
        super(reqType, respType, CommunicationMode.REQUEST_ONE, payloadCodec);
    }

    // HeyService::sayHey -> ServiceReference
    @Override
    Publisher<RESP> invoke(ServiceReference sr, REQ request) {

//        ClientTransport clientTransport = UriTransportRegistry.clientForUri(sr.serviceUri); // 172.1.1.54:qualifier/greetingHello
//        TcpClientTransport clientTransport1 = TcpClientTransport.create(new InetSocketAddress(sr.host(), sr.port()));

        // map<uri, rSocket>

//        client = RSocketFactory.connect()
//                TODO: handle errors
//                .errorConsumer(Throwable::printStackTrace)
//                .transport(clientTransport1)
//                .start()
//                .block();


        ServiceMessage serviceReq = request instanceof ServiceMessage
                ? (ServiceMessage) request
                : ServiceMessage.builder().qualifier(sr.namespace(), sr.action()).build();
        Mono<Payload> payloadMono = client.requestResponse((Payload) payloadCodec.encodeMessage(serviceReq));
        if (respType == ServiceMessage.class) {
            return payloadMono.map(payload -> payloadCodec.decodeMessage(payload)).cast(respType);
        } else {
            return payloadMono.map(payload -> payloadCodec.decodeMessage(payload)).cast(respType);
        }
    }
}
