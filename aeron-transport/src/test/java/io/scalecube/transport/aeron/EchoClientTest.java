package io.scalecube.transport.aeron;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.file.Paths;


class EchoClientTest {

    @Test
    void create() throws Exception {
        EchoClient echoClient = EchoClient.create(
                Paths.get("mediaDirectory"),
                new InetSocketAddress(4040),
                new InetSocketAddress(5050)
        );

        echoClient.run();
        Thread.currentThread().join();
    }
}