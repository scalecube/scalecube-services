package io.scalecube.services;

import static org.junit.Assert.assertEquals;

import io.scalecube.cluster.Cluster;
import io.scalecube.services.annotations.AnnotationServiceProcessor;
import io.scalecube.services.annotations.ServiceProcessor;
import io.scalecube.transport.Transport;

import org.junit.Test;

public class ServiceRegistryImplTest {

  @Test
  public void test_service_registry() {

    Cluster cluster = Cluster.joinAwait();
    Transport transport = Transport.bindAwait();

    ServicesConfig services = ServicesConfig.empty();
    ServiceProcessor serviceProcessor = new AnnotationServiceProcessor();

    ServiceCommunicator sender = new ClusterSender(cluster);
    
    ServiceRegistryImpl registry = new ServiceRegistryImpl(cluster, sender, services, serviceProcessor);

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
    
    cluster.shutdown();
    
  }
}
