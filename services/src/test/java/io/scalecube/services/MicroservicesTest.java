package io.scalecube.services;

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

  @BeforeEach
  void setUp() {
    Mockito.when(serviceTransport.getExecutorService()).thenReturn(executorService);
    Mockito.when(serviceTransport.getClientTransport(Mockito.any())).thenReturn(clientTransport);
    Mockito.when(serviceTransport.getServerTransport(Mockito.any())).thenReturn(serverTransport);
  }

  @Test
  public void testSpecifiedPortIsAlreadyUse() {
    int port = 12345;
    Microservices node1 = Microservices.builder().servicePort(port).startAwait();

    StepVerifier.create(Microservices.builder().servicePort(port).start())
        .expectErrorMessage("bind(..) failed: Address already in use")
        .verify();

    node1.shutdown();
  }

  @Test
  public void testThrowExceptionWhileStartServiceDiscovery() {
    String expectedErrorMessage = "expected error message";
    Mockito.when(serviceDiscovery.start(Mockito.any()))
        .thenThrow(new RuntimeException(expectedErrorMessage));

    StepVerifier.create(Microservices.builder()
        .discovery(serviceDiscovery)
        .transport(serviceTransport)
        .start())
        .expectErrorMessage(expectedErrorMessage)
        .verify();

    Mockito.verify(serverTransport, Mockito.atLeastOnce()).stop();
    Mockito.verify(serviceDiscovery, Mockito.atLeastOnce()).shutdown();
    Mockito.verify(serviceTransport, Mockito.atLeastOnce()).shutdown(Mockito.eq(executorService));
  }

}
