package io.scalecube.services;

import static org.mockito.ArgumentMatchers.any;

import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceTransport;
import java.util.concurrent.ExecutorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.WARN)
public class MicroservicesTest {

  @Mock
  private ServiceDiscovery serviceDiscovery;
  @Mock
  private ServiceTransport serviceTransport;
  @Mock
  private ServerTransport serverTransport;
  @Mock
  private ClientTransport clientTransport;
  @Mock
  private ExecutorService selectorExecutor;
  @Mock
  private ExecutorService workerExecutor;

  @BeforeEach
  public void setUp() {
    Mockito.when(serviceTransport.getSelectorThreadPool()).thenReturn(selectorExecutor);
    Mockito.when(serviceTransport.getWorkerThreadPool(any())).thenReturn(workerExecutor);
    Mockito.when(serviceTransport.getClientTransport(any(), any())).thenReturn(clientTransport);
    Mockito.when(serviceTransport.getServerTransport(any(), any())).thenReturn(serverTransport);
  }

  @Test
  public void testServiceTransportNotStarting() {
    String expectedErrorMessage = "expected error message";
    Mockito.when(serverTransport.bindAwait(any(), any()))
        .thenThrow(new RuntimeException(expectedErrorMessage));

    StepVerifier.create(Microservices.builder().transport(serviceTransport).start())
        .expectErrorMessage(expectedErrorMessage)
        .verify();
  }

  @Test
  public void testServiceDiscoveryNotStarting() {
    String expectedErrorMessage = "expected error message";
    Mockito.when(serviceDiscovery.start(any()))
        .thenThrow(new RuntimeException(expectedErrorMessage));

    StepVerifier.create(
        Microservices.builder().discovery(serviceDiscovery).transport(serviceTransport).start())
        .expectErrorMessage(expectedErrorMessage)
        .verify();

    Mockito.verify(serverTransport, Mockito.atLeastOnce()).stop();
    Mockito.verify(serviceDiscovery, Mockito.atLeastOnce()).shutdown();
    Mockito.verify(serviceTransport, Mockito.atLeastOnce())
        .shutdown(Mockito.eq(selectorExecutor), Mockito.eq(workerExecutor));
  }
}
