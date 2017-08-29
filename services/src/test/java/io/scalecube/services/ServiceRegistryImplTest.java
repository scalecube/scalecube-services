package io.scalecube.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.cluster.Cluster;
import io.scalecube.services.annotations.AnnotationServiceProcessor;
import io.scalecube.services.annotations.ServiceProcessor;
import io.scalecube.testlib.BaseTest;
import io.scalecube.transport.Transport;

import org.junit.Test;

public class ServiceRegistryImplTest extends BaseTest {

  @Test
  public void test_service_registry() {

    Cluster cluster = Cluster.joinAwait();

    ServicesConfig services = ServicesConfig.empty();
    ServiceProcessor serviceProcessor = new AnnotationServiceProcessor();

    ServiceCommunicator sender = new TransportServiceCommunicator(Transport.bindAwait());

    ServiceRegistryImpl registry = new ServiceRegistryImpl(cluster, sender, services, serviceProcessor);

    assertTrue(registry.services().isEmpty());
    
    cluster.shutdown();
    sender.shutdown();

  }

  @Test
  public void test_service_registry_errors() {
    Cluster cluster = Cluster.joinAwait();
    ServiceCommunicator sender = new TransportServiceCommunicator(Transport.bindAwait());
    ServiceProcessor serviceProcessor = new AnnotationServiceProcessor();
    ServicesConfig services = ServicesConfig.empty();

    try {
      new ServiceRegistryImpl(null, sender, services, serviceProcessor);
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: cluster can't be null");
    }

    try {
      new ServiceRegistryImpl(cluster, null, services, serviceProcessor);
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: transport can't be null");
    }

    try {
      new ServiceRegistryImpl(cluster, sender, null, serviceProcessor);
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: services can't be null");
    }

    try {
      new ServiceRegistryImpl(cluster, sender, services, null);
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: serviceProcessor can't be null");
    }
    sender.shutdown();
    cluster.shutdown();

  }

}
