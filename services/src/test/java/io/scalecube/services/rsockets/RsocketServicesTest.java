package io.scalecube.services.rsockets;

import io.scalecube.services.Microservices;

import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;

import reactor.core.publisher.Mono;

public class RsocketServicesTest {

    public static final String BANANA = "banana";
    private static final Duration TIMEOUT = Duration.ofSeconds(3);

    @Test
    public void testSimpleService() {

        Microservices gateway = Microservices.builder().build();

        HeyService service = new HeyServiceImpl();

        Microservices.builder()
                .seeds(gateway.serviceAddress())
                .services().build();

        HeyService proxy = gateway.forService(HeyService.class);

        Mono<HeyPayload> banana = proxy.revert(new HeyPayload(BANANA));
        HeyPayload result = banana.block(TIMEOUT);
        Assert.assertNotNull(result);
        Assert.assertEquals(BANANA, new StringBuffer(result.getText()).reverse().toString());
    }
}
