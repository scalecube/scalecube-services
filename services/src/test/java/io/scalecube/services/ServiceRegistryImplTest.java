package io.scalecube.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.cluster.Cluster;
import io.scalecube.testlib.BaseTest;
import io.scalecube.transport.Transport;

import org.junit.Test;

public class ServiceRegistryImplTest extends BaseTest {

  @Test
  public void test_service_registry() {

    Cluster cluster = Cluster.joinAwait();

    ServicesConfig services = ServicesConfig.empty();

    ServiceCommunicator sender = new ServiceTransport(Transport.bindAwait());

    ServiceRegistryImpl registry = new ServiceRegistryImpl(cluster, sender, services);

    assertTrue(registry.services().isEmpty());
    
    cluster.shutdown();
    sender.shutdown();

  }

  @Test
  public void test_service_registry_errors() {
    Cluster cluster = Cluster.joinAwait();
    ServiceCommunicator sender = new ServiceTransport(Transport.bindAwait());
    
    ServicesConfig services = ServicesConfig.empty();

    try {
      new ServiceRegistryImpl(null, sender, services);
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: cluster can't be null");
    }

    try {
      new ServiceRegistryImpl(cluster, null, services);
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: transport can't be null");
    }

    try {
      new ServiceRegistryImpl(cluster, sender, null);
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: services can't be null");
    }

    sender.shutdown();
    cluster.shutdown();

  }

}
