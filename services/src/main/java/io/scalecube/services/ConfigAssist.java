package io.scalecube.services;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.membership.MembershipConfig;
import io.scalecube.transport.Address;
import io.scalecube.transport.TransportConfig;

import java.util.Map;

public class ConfigAssist {

  /**
   * creates ClusterConfig instance by given port, metadata.
   * 
   * @param port provided port requested in the config.
   * @param metadata provided metatadata requested in the config.
   * @return newly created ClusterConfig.
   */
  public static ClusterConfig create(String listenAddress, int port, boolean portAutoIncrement,
      Map<String, String> metadata) {
    return ClusterConfig.builder()
        .transportConfig(
            TransportConfig.builder().port(port)
                .listenAddress(listenAddress)
                .portAutoIncrement(portAutoIncrement)
                .build())
        .membershipConfig(MembershipConfig.builder()
            .metadata(metadata)
            .build())
        .build();
  }

  public static ClusterConfig create(String listenAddress, Integer port, boolean portAutoIncrement, Address[] seeds,
      Map<String, String> metadata) {
    return ClusterConfig.builder().build();
  }

  /**
   * creates ClusterConfig instance by given metadata.
   * 
   * @param metadata provided metatadata requested in the config.
   * @return newly created ClusterConfig.
   */
  public static ClusterConfig create(String listenAddress, Map<String, String> metadata) {
    return ClusterConfig.builder()
        .transportConfig(
            TransportConfig.builder()
                .listenAddress(listenAddress)
                .build())
        .membershipConfig(MembershipConfig.builder()
            .metadata(metadata)
            .build())
        .build();
  }

  /**
   * creates ClusterConfig instance by given seeds, metadata.
   * 
   * @param seeds provided seeds requested in the config.
   * @param metadata provided metatadata requested in the config.
   * @return newly created ClusterConfig.
   */
  public static ClusterConfig create(String listenAddress, Address[] seeds, Map<String, String> metadata) {
    return ClusterConfig.builder()
        .transportConfig(
            TransportConfig.builder()
                .listenAddress(listenAddress)
                .build())
        .membershipConfig(MembershipConfig.builder()
            .seedMembers(seeds)
            .metadata(metadata)
            .build())
        .build();
  }

  /**
   * creates ClusterConfig instance by given port, seeds, metadata.
   * 
   * @param port provided port requested in the config.
   * @param seeds provided seeds requested in the config.
   * @param metadata provided metatadata requested in the config.
   * @return newly created ClusterConfig.
   */
  public static ClusterConfig create(String listenAddress, int port, boolean portAutoIncrement, Address[] seeds,
      Map<String, String> metadata) {
    return ClusterConfig.builder()
        .transportConfig(
            TransportConfig.builder()
                .listenAddress(listenAddress)
                .port(port)
                .portAutoIncrement(portAutoIncrement)
                .build())
        .membershipConfig(MembershipConfig.builder()
            .seedMembers(seeds)
            .metadata(metadata)
            .build())
        .build();
  }


}
