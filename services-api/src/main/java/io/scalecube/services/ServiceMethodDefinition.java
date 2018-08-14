package io.scalecube.services;

import java.util.Collections;
import java.util.Map;

public class ServiceMethodDefinition {

  private String action;
  private Map<String, String> tags;
  private CommunicationMode communicationMode;

  /**
   * @deprecated exposed only for deserialization purpose.
   */
  public ServiceMethodDefinition() {}

  public ServiceMethodDefinition(String action, CommunicationMode communicationMode) {
    this(action, Collections.emptyMap(), communicationMode);
  }

  public ServiceMethodDefinition(String action, Map<String, String> tags, CommunicationMode communicationMode) {
    this.action = action;
    this.tags = tags;
    this.communicationMode = communicationMode;
  }

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

  public CommunicationMode getCommunicationMode() {
    return communicationMode;
  }

  public void setCommunicationMode(CommunicationMode communicationMode) {
    this.communicationMode = communicationMode;
  }

  @Override
  public String toString() {
    return "ServiceMethodDefinition{" +
        "action='" + action + '\'' +
        ", tags=" + tags +
        '}';
  }
}
