package io.scalecube.rsockets;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.uri.UriTransportRegistry;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;

public class MonoInvoker<REQ, RESP> extends ActionMethodInvoker<REQ, RESP> {

    public MonoInvoker(Class<REQ> reqType, Class<RESP> respType, PayloadCodec payloadCodec) {
        super(reqType, respType, CommunicationMode.MONO, payloadCodec);
    }

    // HeyService::sayHey -> ServiceReference
    @Override
    Publisher<RESP> invoke(ServiceReference sr, REQ request) {

//        ClientTransport clientTransport = UriTransportRegistry.clientForUri(sr.serviceUri); // 172.1.1.54:qualifier/greetingHello
//        TcpClientTransport clientTransport1 = TcpClientTransport.create(new InetSocketAddress(sr.host(), sr.port()));

        // map<uri, rSocket>

//        client = RSocketFactory.connect()
                TODO: handle errors
//                .errorConsumer(Throwable::printStackTrace)
//                .transport(clientTransport1)
//                .start()
//                .block();


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
