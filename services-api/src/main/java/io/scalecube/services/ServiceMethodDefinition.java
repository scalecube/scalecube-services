package io.scalecube.services;

import java.util.Collections;
import java.util.Map;

/**
 * A Service Method Definition is a single method definition of a service inside service
 * registration.
 */
public class ServiceMethodDefinition {

  private String action;
  private Map<String, String> tags;
  private boolean auth;

  /**
   * Constructor for SerDe.
   *
   * @deprecated exposed only for de/serialization purpose.
   */
  public ServiceMethodDefinition() {}

  /**
   * Create a new Service Method Definition.
   *
   * @param action method name
   */
  public ServiceMethodDefinition(String action) {
    this(action, Collections.emptyMap(), false);
  }

  /**
   * Create a new Service Method Definition.
   *
   * @param action method name
   * @param tags tags of this method
   * @param auth is method protected by authentication
   */
  public ServiceMethodDefinition(String action, Map<String, String> tags, boolean auth) {
    this.action = action;
    this.tags = tags;
    this.auth = auth;
  }

  /**
   * a generic definition for method name.
   *
   * @return the method name
   */
  public String getAction() {
    return action;
  }

  public ServiceMethodDefinition setAction(String action) {
    this.action = action;
    return this;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public ServiceMethodDefinition setTags(Map<String, String> tags) {
    this.tags = tags;
    return this;
  }

  public boolean isAuth() {
    return auth;
  }

  public ServiceMethodDefinition setAuth(boolean auth) {
    this.auth = auth;
    return this;
  }

  @Override
  public String toString() {
    return "ServiceMethodDefinition{"
        + "action='"
        + action
        + '\''
        + ", tags="
        + tags
        + ", auth="
        + auth
        + '}';
  }
}
