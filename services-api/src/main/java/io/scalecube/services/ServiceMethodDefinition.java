package io.scalecube.services;

import java.util.Map;

public class ServiceMethodDefinition {

    private String action;
    private String contentType;
    private String communicationMode;
    private Map<String, String> tags;


    /**
     * @deprecated exposed only for deserialization purpose.
     */
    public ServiceMethodDefinition() {
    }

    public ServiceMethodDefinition(String action, String contentType, String communicationMode, Map<String, String> tags) {
        this.action = action;
        this.contentType = contentType;
        this.communicationMode = communicationMode;
        this.tags = tags;
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

    public String getCommunicationMode() {
        return communicationMode;
    }

    public ServiceMethodDefinition setCommunicationMode(String communicationMode) {
        this.communicationMode = communicationMode;
        return this;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public ServiceMethodDefinition setTags(Map<String, String> tags) {
        this.tags = tags;
        return this;
    }

    @Override
    public String toString() {
        return "ServiceMethodDefinition{" +
                "action='" + action + '\'' +
                ", contentType='" + contentType + '\'' +
                ", communicationMode='" + communicationMode + '\'' +
                ", tags=" + tags +
                '}';
    }
}
