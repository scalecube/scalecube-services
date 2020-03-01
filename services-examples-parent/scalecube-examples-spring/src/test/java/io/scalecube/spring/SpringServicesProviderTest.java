package io.scalecube.spring;

import io.scalecube.services.ScaleCube;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class SpringServicesProviderTest {

    private static ScaleCube gateway;
    private static ScaleCube provider;

    @BeforeAll
    public static void setup() {
        Hooks.onOperatorDebug();
        gateway = gateway();
        provider = serviceProvider();
    }

    @AfterAll
    public static void tearDown() {
        try {
            gateway.shutdown().block();
        } catch (Exception ignore) {
            // no-op
        }

        try {
            provider.shutdown().block();
        } catch (Exception ignore) {
            // no-op
        }
    }

    private static ScaleCube gateway() {
        return ScaleCube.builder()
                .discovery(ScalecubeServiceDiscovery::new)
                .transport(RSocketServiceTransport::new)
                .services(new SpringServicesProvider())
                .startAwait();
    }

    private static ScaleCube serviceProvider() {
        return ScaleCube.builder()
                .discovery(SpringServicesProviderTest::serviceDiscovery)
                .transport(RSocketServiceTransport::new)
                .services(new SpringServicesProvider.SimpleService() {
                    @Override
                    public Mono<Long> get() {
                        return Mono.just(1L);
                    }
                })
                .startAwait();
    }

    @Test
    public void test_remote_greeting_request_completes_before_timeout() {

        SpringServicesProvider.LocalService service = gateway.call().api(SpringServicesProvider.LocalService.class);

        // call the service.
        Mono<Long> result =
                service.get();
        Long block = result.block(Duration.ofSeconds(10));
        assertEquals(-1L, block.longValue());
    }

    private static ServiceDiscovery serviceDiscovery(ServiceEndpoint endpoint) {
        return new ScalecubeServiceDiscovery(endpoint)
                .membership(cfg -> cfg.seedMembers(gateway.discovery().address()));
    }
}