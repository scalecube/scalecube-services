package io.scalecube.services;

import static org.mockito.ArgumentMatchers.any;

import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceTransport;
import java.util.concurrent.ExecutorService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
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
  private ExecutorService executorService;

  @Test
  public void testServiceTransportNotStarting() {
    Mockito.when(serviceTransport.getExecutorService()).thenReturn(executorService);
    Mockito.when(serviceTransport.getClientTransport(any())).thenReturn(clientTransport);
    Mockito.when(serviceTransport.getServerTransport(any())).thenReturn(serverTransport);

    String expectedErrorMessage = "expected error message";
    Mockito.when(serverTransport.bindAwait(any(), any()))
        .thenThrow(new RuntimeException(expectedErrorMessage));

    StepVerifier.create(Microservices.builder().transport(serviceTransport).start())
        .expectErrorMessage(expectedErrorMessage)
        .verify();
  }

  @Test
  public void testServiceDiscoveryNotStarting() {
    Mockito.when(serviceTransport.getExecutorService()).thenReturn(executorService);
    Mockito.when(serviceTransport.getClientTransport(any())).thenReturn(clientTransport);
    Mockito.when(serviceTransport.getServerTransport(any())).thenReturn(serverTransport);

    String expectedErrorMessage = "expected error message";
    Mockito.when(serviceDiscovery.start(any()))
        .thenThrow(new RuntimeException(expectedErrorMessage));

    StepVerifier.create(
        Microservices.builder().discovery(serviceDiscovery).transport(serviceTransport).start())
        .expectErrorMessage(expectedErrorMessage)
        .verify();

    Mockito.verify(serverTransport, Mockito.atLeastOnce()).stop();
    Mockito.verify(serviceDiscovery, Mockito.atLeastOnce()).shutdown();
    Mockito.verify(serviceTransport, Mockito.atLeastOnce()).shutdown(Mockito.eq(executorService));
  }
}
