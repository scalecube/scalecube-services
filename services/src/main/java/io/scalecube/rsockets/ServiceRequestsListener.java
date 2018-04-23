package io.scalecube.rsockets;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

import reactor.core.publisher.Mono;

public class ServiceRequestsListener {

    private static Logger LOGGER = LoggerFactory.getLogger(ServiceRequestsListener.class);
    private final int port;
    private NettyContextCloseable server;

    public ServiceRequestsListener(int port) {
        this.port = port;
    }

    public InetSocketAddress start() {
        TcpServerTransport serverTransport = TcpServerTransport.create(port);
        server = RSocketFactory.receive()
                .errorConsumer(t -> LOGGER.error("Error on server socket: ", t))
                .acceptor(
                        (setup, sendingSocket) -> {
                            sendingSocket
                                    .onClose()
                                    .doFinally(signalType -> LOGGER.info("Sending socket closed: {}", signalType))
                                    .subscribe();

                            return Mono.just(
                                    new AbstractRSocket() {
                                        @Override
                                        public Mono<Payload> requestResponse(Payload payload) {
                                            return Mono.just(DefaultPayload.create("RESPONSE", "METADATA"))
                                                    .doOnSubscribe(s -> LOGGER.info("Request rcvd: {}", payload));
                                        }
                                    });
                        })
                .transport(serverTransport)
                .start()
                .block();
        return server.address();
    }

    public Mono<Void> stop() {
        server.dispose();
        return server.onClose();
    }
}
