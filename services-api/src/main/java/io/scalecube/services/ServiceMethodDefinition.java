package io.scalecube.services;

import java.util.Map;

public class ServiceMethodDefinition {

  private String action;
  private String contentType;
  private Map<String, String> tags;
  private CommunicationMode communicationMode;

  /**
   * @deprecated exposed only for deserialization purpose.
   */
  public ServiceMethodDefinition() {}

  public ServiceMethodDefinition(String action, String contentType, Map<String, String> tags, CommunicationMode communicationMode) {
    this.action = action;
    this.contentType = contentType;
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

  public String getContentType() {
    return contentType;
  }

  public ServiceMethodDefinition setContentType(String contentType) {
    this.contentType = contentType;
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
        ", contentType='" + contentType + '\'' +
        ", tags=" + tags +
        '}';
  }
}
