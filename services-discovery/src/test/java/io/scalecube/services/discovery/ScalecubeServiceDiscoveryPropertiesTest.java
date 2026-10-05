package io.scalecube.services.discovery;

import static io.scalecube.cluster.ClusterConfig.MEMBER_ALIAS_PROP_NAME;
import static io.scalecube.cluster.membership.MembershipConfig.NAMESPACE_PROP_NAME;
import static io.scalecube.cluster.membership.MembershipConfig.SEED_MEMBERS_PROP_NAME;
import static io.scalecube.cluster.transport.api.TransportConfig.PORT_PROP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class ScalecubeServiceDiscoveryPropertiesTest {

  @Test
  void testClusterConfigIsBuiltFromProperties() {
    final var properties = new Properties();
    properties.setProperty(MEMBER_ALIAS_PROP_NAME, "alias");
    properties.setProperty(SEED_MEMBERS_PROP_NAME, "host1:4801,host2:4801");
    properties.setProperty(NAMESPACE_PROP_NAME, "site/env");
    properties.setProperty(PORT_PROP_NAME, "4801");

    final var config = new ScalecubeServiceDiscovery(properties).clusterConfig();

    assertEquals("alias", config.memberAlias(), "memberAlias");
    assertEquals(
        List.of("host1:4801", "host2:4801"), config.membershipConfig().seedMembers(), "seeds");
    assertEquals("site/env", config.membershipConfig().namespace(), "namespace");
    assertEquals(4801, config.transportConfig().port(), "port");
  }

  @Test
  void testSettersMutateInPlace() {
    final var discovery = new ScalecubeServiceDiscovery(new Properties());
    final var config = discovery.clusterConfig();

    assertSame(discovery, discovery.membership(opts -> opts.seedMembers("host:4801")));
    assertSame(config, discovery.clusterConfig());
    assertEquals(List.of("host:4801"), config.membershipConfig().seedMembers());
  }
}
