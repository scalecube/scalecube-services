package io.scalecube.services;

import io.scalecube.net.Address;
import io.scalecube.services.api.Qualifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Service reference. This is merge of service method information together with service registration
 * and with service endpoint.
 */
public class ServiceReference {

  private final String qualifier;
  private final String endpointId;
  private final String namespace;
  private final Set<String> contentTypes;
  private final Map<String, String> tags;
  private final String action;
  private final Address address;

  /**
   * Constructor for service reference.
   *
   * @param serviceMethodDefinition service method info
   * @param serviceRegistration service registration
   * @param serviceEndpoint service node info
   */
  public ServiceReference(
      ServiceMethodDefinition serviceMethodDefinition,
      ServiceRegistration serviceRegistration,
      ServiceEndpoint serviceEndpoint) {
    this.endpointId = serviceEndpoint.id();
    this.namespace = serviceRegistration.namespace();
    this.contentTypes = Collections.unmodifiableSet(serviceEndpoint.contentTypes());
    this.tags = mergeTags(serviceMethodDefinition, serviceRegistration, serviceEndpoint);
    this.action = serviceMethodDefinition.getAction();
    this.qualifier = Qualifier.asString(namespace, action);
    this.address = serviceEndpoint.address();
  }

  public String qualifier() {
    return this.qualifier;
  }

  public String endpointId() {
    return endpointId;
  }

  public String namespace() {
    return namespace;
  }

  public Set<String> contentTypes() {
    return contentTypes;
  }

  public Map<String, String> tags() {
    return tags;
  }

  public String action() {
    return action;
  }

  public Address address() {
    return this.address;
  }

  private Map<String, String> mergeTags(
      ServiceMethodDefinition serviceMethodDefinition,
      ServiceRegistration serviceRegistration,
      ServiceEndpoint serviceEndpoint) {
    Map<String, String> tags = new HashMap<>();
    tags.putAll(serviceEndpoint.tags());
    tags.putAll(serviceRegistration.tags());
    tags.putAll(serviceMethodDefinition.getTags());
    return tags;
  }

  @Override
  public String toString() {
    return "ServiceReference{"
        + "qualifier='"
        + qualifier
        + '\''
        + ", endpointId='"
        + endpointId
        + '\''
        + ", address='"
        + address
        + '\''
        + ", namespace='"
        + namespace
        + '\''
        + ", contentTypes='"
        + contentTypes
        + '\''
        + ", tags="
        + tags
        + ", action='"
        + action
        + '\''
        + '}';
  }
}
