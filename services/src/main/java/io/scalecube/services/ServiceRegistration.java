package io.scalecube.services;

import java.util.Arrays;
import java.util.List;

public class ServiceRegistration {

  private final String memberId;
  private final String serviceName;
  private final String ip;
  private final int port;
  private final List<String> tags = Arrays.asList("microservice");

  public String memberId() {
    return memberId;
  }

  public String serviceName() {
    return serviceName;
  }

  public String ip() {
    return ip;
  }

  public int port() {
    return port;
  }

  private ServiceRegistration(String memberId, String serviceName, String ip, int port, String... tags) {
    this.memberId = memberId;
    this.serviceName = serviceName;
    this.ip = ip;
    this.port = port;

    for (String tag : tags) {
      if (!this.tags.contains(tag))
        this.tags.add(tag);
    }
  }

  public static ServiceRegistration create(String memberId, String serviceName, String ip, int port, String... tags) {
    return new ServiceRegistration(memberId, serviceName, ip, port, tags);
  }

  public String[] tags() {
    return tags.toArray(new String[tags.size()]);
  }


}
