package io.scalecube.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.testlib.BaseTest;
import io.scalecube.transport.TransportConfig;

import org.junit.Test;

public class ServiceConfigTest extends BaseTest {

  @Test
  public void test_services_config_port_increment_false() {

    Microservices node = Microservices.builder()
        .serviceTransport(TransportConfig.builder()
            .port(3000)
            .portAutoIncrement(false)
            .build())
        .clusterConfig(ClusterConfig.builder()
            .port(4000)
            .portAutoIncrement(false)) // cluster config
        .build(); // node


    assertEquals(node.sender().address().port(), 3000);
    assertEquals(node.cluster().address().port(), 4000);

    node.shutdown();

  }

  @Test
  public void test_ServiceConfig_builder() {

    Microservices.Builder builder = Microservices.builder();
    ServicesConfig config =
        ServicesConfig.builder(builder).service(new GreetingServiceImpl()).tag("a", "b").add().create();
    builder.build();

    assertTrue(!config.services().isEmpty());
    assertTrue(!config.services().get(0).getDefinitions().isEmpty());
  }

}
