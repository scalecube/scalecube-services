package io.scalecube.services;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;

import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceTransport;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.WARN)
public class MicroservicesTest {

  @Mock private ServiceDiscovery serviceDiscovery;
  @Mock private ServiceTransport serviceTransport;
  @Mock private ServerTransport serverTransport;
  @Mock private ClientTransport clientTransport;
  @Mock private ExecutorService workerExecutor;

  /** Setup. */
  @BeforeEach
  public void setUp() {
    Mockito.when(serviceTransport.getWorkerThreadPool(anyInt())).thenReturn(workerExecutor);
    Mockito.when(serviceTransport.getClientTransport(any())).thenReturn(clientTransport);
    Mockito.when(serviceTransport.getServerTransport(any())).thenReturn(serverTransport);
  }

  @Test
  public void testServiceTransportNotStarting() {
    String expectedErrorMessage = "expected error message";

    Mockito.when(serverTransport.bind(anyInt(), any()))
        .thenReturn(Mono.error(new RuntimeException(expectedErrorMessage)));

    StepVerifier.create(
            Microservices.builder()
                .transport(options -> options.transport(serviceTransport))
                .start())
        .expectErrorMessage(expectedErrorMessage)
        .verify();
  }

  @Test
  public void testServiceDiscoveryNotStarting() {
    String expectedErrorMessage = "expected error message";
    Mockito.when(serviceDiscovery.start(any()))
        .thenThrow(new RuntimeException(expectedErrorMessage));
    Mockito.when(serverTransport.bind(anyInt(), any()))
        .thenReturn(Mono.just(new InetSocketAddress(0)));

    StepVerifier.create(
            Microservices.builder()
                .discovery(serviceDiscovery)
                .transport(options -> options.transport(serviceTransport))
                .start())
        .expectErrorMessage(expectedErrorMessage)
        .verify();

    Mockito.verify(serverTransport, Mockito.atLeastOnce()).stop();
    Mockito.verify(serviceDiscovery, Mockito.atLeastOnce()).shutdown();
    Mockito.verify(serviceTransport, Mockito.atLeastOnce()).shutdown(Mockito.eq(workerExecutor));
  }
}
