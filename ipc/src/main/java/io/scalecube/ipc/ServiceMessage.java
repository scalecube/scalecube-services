package io.scalecube.ipc;

import java.util.Objects;

public final class ServiceMessage {

  public static final String QUALIFIER_NAME = "q";
  public static final String SENDER_ID_NAME = "senderId";
  public static final String STREAM_ID_NAME = "streamId";
  public static final String DATA_NAME = "data";

  private final String qualifier;
  private final String senderId;
  private final String streamId;
  private final Object data;

  //// builders

  public static Builder withQualifier(String qualifier) {
    return builder().qualifier(qualifier);
  }

  public static Builder withQualifier(Qualifier qualifier) {
    return builder().qualifier(qualifier);
  }

  public static Builder copyFrom(ServiceMessage message) {
    return withHeaders(message).qualifier(message.qualifier).data(message.data);
  }

  public static Builder withHeaders(ServiceMessage message) {
    return builder().streamId(message.streamId).senderId(message.senderId);
  }

  public static Builder builder() {
    return new Builder();
  }

  private ServiceMessage(Builder builder) {
    this.qualifier = builder.qualifier;
    this.data = builder.data;
    this.senderId = builder.senderId;
    this.streamId = builder.streamId;
  }

  //// accessors

  public String getQualifier() {
    return qualifier;
  }

  public String getSenderId() {
    return senderId;
  }

  public String getStreamId() {
    return streamId;
  }

  public Object getData() {
    return data;
  }

  public boolean hasData() {
    return data != null;
  }

  public boolean hasSenderId() {
    return senderId != null && !senderId.isEmpty();
  }

  public boolean dataOfType(Class<?> clazz) {
    return data != null && data.getClass().isInstance(clazz);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    ServiceMessage message = (ServiceMessage) obj;
    return Objects.equals(qualifier, message.qualifier)
        && Objects.equals(data, message.data)
        && Objects.equals(senderId, message.senderId)
        && Objects.equals(streamId, message.streamId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(qualifier, senderId, streamId, data);
  }

  @Override
  public String toString() {
    return "Message [q=" + qualifier
        + ", senderId=" + senderId
        + ", streamId=" + streamId
        + ", data=" + prepareDataString()
        + "]";
  }

  private String prepareDataString() {
    if (data == null) {
      return "null";
    } else if (data instanceof byte[]) {
      return "b-" + ((byte[]) data).length;
    } else if (data instanceof String) {
      return "s-" + ((String) data).length();
    } else if (data instanceof ErrorData) {
      return data.toString();
    } else {
      return data.getClass().getName();
    }
  }

  public static final class Builder {

    private String qualifier;
    private String senderId;
    private String streamId;
    private Object data;

    private Builder() {}

    public Builder qualifier(String qualifier) {
      this.qualifier = qualifier;
      return this;
    }

    public Builder qualifier(Qualifier qualifier) {
      this.qualifier = qualifier.asString();
      return this;
    }

    public Builder senderId(String senderId) {
      this.senderId = senderId;
      return this;
    }

    public Builder streamId(String streamId) {
      this.streamId = streamId;
      return this;
    }

    public Builder data(Object data) {
      this.data = data;
      return this;
    }

    public ServiceMessage build() {
      return new ServiceMessage(this);
    }
  }
}
