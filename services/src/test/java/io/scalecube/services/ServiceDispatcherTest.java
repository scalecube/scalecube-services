package io.scalecube.services;

import io.scalecube.cluster.Cluster;
import io.scalecube.services.annotations.AnnotationServiceProcessor;
import io.scalecube.services.annotations.ServiceProcessor;
import io.scalecube.transport.Message;

import org.junit.Test;

public class ServiceDispatcherTest {

  @Test
  public void test_service_dispatcher() {

    Cluster cluster = Cluster.joinAwait();
    ServiceCommunicator sender = new ClusterServiceCommunicator(cluster);
    ServiceProcessor serviceProcessor = new AnnotationServiceProcessor();
    ServicesConfig services = ServicesConfig.empty();
    ServiceRegistry registry = new ServiceRegistryImpl(cluster, sender, services, serviceProcessor);

    ServiceDispatcher dispatcher = new ServiceDispatcher(sender, registry);

    cluster.send(cluster.address(), Message.builder().data("1").build());
    cluster.send(cluster.address(), Message.builder()
        .header(ServiceHeaders.SERVICE_REQUEST, "none")
        .data("1").build());

    cluster.shutdown(); 
  }
}
