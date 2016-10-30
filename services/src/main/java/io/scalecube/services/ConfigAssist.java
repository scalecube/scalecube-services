package io.scalecube.services;

import java.util.Map;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.membership.MembershipConfig;
import io.scalecube.transport.Address;
import io.scalecube.transport.TransportConfig;

public class ConfigAssist {

  public static ClusterConfig create(Integer port, Map<String, String> metadata) {
    return ClusterConfig.builder()
        .transportConfig(
            TransportConfig.builder().port(port)
                .build())
        .membershipConfig(MembershipConfig.builder()
            .metadata(metadata)
            .build())
        .build();
  }

  public static ClusterConfig create() {
    return ClusterConfig.builder().build();
  }

  public static ClusterConfig create(Map<String, String> metadata) {
    return ClusterConfig.builder()
        .membershipConfig(MembershipConfig.builder()
            .metadata(metadata)
            .build())
        .build();
  }

  public static ClusterConfig create(Address[] seeds, Map<String, String> metadata) {
    return ClusterConfig.builder()
        .membershipConfig(MembershipConfig.builder()
            .seedMembers(seeds)
            .metadata(metadata)
            .build())
        .build();
  }

  public static ClusterConfig create(int port, Address[] seeds, Map<String, String> metadata) {
    return ClusterConfig.builder()
        .transportConfig(
            TransportConfig.builder()
                .port(port)
                .build())
        .membershipConfig(MembershipConfig.builder()
            .seedMembers(seeds)
            .metadata(metadata)
            .build())
        .build();
  }


}
