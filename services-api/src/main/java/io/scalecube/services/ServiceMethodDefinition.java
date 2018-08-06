package io.scalecube.services;

import java.util.Collections;
import java.util.Map;

/**
 * A Service Method Definition is a single method definition of a service inside service
 * registration.
 * 
 */
public class ServiceMethodDefinition {

  private String action;
  private String contentType;
  private Map<String, String> tags;
  private CommunicationMode communicationMode;

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
   * @param contentType the data format for payload.
   * @param communicationMode the communication mode: e.g:
   *        {@link CommunicationMode#REQUEST_RESPONSE}
   */
  public ServiceMethodDefinition(String action, String contentType,
      CommunicationMode communicationMode) {
    this(action, contentType, Collections.emptyMap(), communicationMode);
  }


  /**
   * Create a new Service Method Definition.
   * 
   * @param action method name
   * @param contentType the data format for payload.
   * @param tags tags of this method
   * @param communicationMode the communication mode: e.g:
   *        {@link CommunicationMode#REQUEST_RESPONSE}
   */
  public ServiceMethodDefinition(String action, String contentType, Map<String, String> tags,
      CommunicationMode communicationMode) {
    this.action = action;
    this.contentType = contentType;
    this.tags = tags;
    this.communicationMode = communicationMode;
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
    return "ServiceMethodDefinition{"
        + "action='" + action + '\''
        + ", contentType='" + contentType + '\''
        + ", tags=" + tags
        + '}';
  }
}
