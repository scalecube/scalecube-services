package io.scalecube.services;

/**
 * Helper class used to register service with tags as metadata in the scalecube cluster. parsing from service info to
 * json and back.
 */
public class ServiceInfo {

  private String serviceName;

  private Tag[] tags;

  public ServiceInfo() {
    // default contractor used for json serialization.
  }

  public ServiceInfo(String serviceName, Tag[] tags) {
    this.serviceName = serviceName;
    this.tags = tags;
  }

  public Tag[] getTags() {
    return tags;
  }

  public String getServiceName() {
    return serviceName;
  }

  private void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  private void setTags(Tag[] tags) {
    this.tags = tags;
  }
}
