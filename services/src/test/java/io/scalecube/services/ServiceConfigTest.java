package io.scalecube.services;

import static org.junit.Assert.assertTrue;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.transport.TransportConfig;

import org.junit.Test;

public class ServiceConfigTest {

  @Test
  public void test_services_config_port_increment_false() {

    Microservices node = Microservices.builder().clusterConfig(
        ClusterConfig.builder()

            .transportConfig(TransportConfig.builder()
                .port(3000)
                .portAutoIncrement(false)
                .build())) // cluster config
        
        .build(); // node 

    assertTrue(node.cluster().address().port() == 3000);

  }

}
