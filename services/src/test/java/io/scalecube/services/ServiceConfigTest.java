package io.scalecube.services;

import static org.junit.Assert.assertEquals;
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

    assertEquals(node.cluster().address().port() , 3000);

    node.cluster().shutdown();

  }

  @Test
  public void test_ServiceConfig_builder() {

    Microservices.Builder builder = Microservices.builder();
    ServicesConfig config =
        ServicesConfig.builder(builder).service(new GreetingServiceImpl()).tag("a", "b").add().create();
    builder.build();

    assertTrue(!config.getServiceConfigs().isEmpty());
    assertTrue(!config.getServiceConfigs().get(0).getDefinitions().isEmpty());
  }

}
