package io.scalecube.streams;

import java.util.Objects;

public final class StreamMessage {

  public static final String QUALIFIER_NAME = "q";
  public static final String SUBJECT_NAME = "subject";
  public static final String DATA_NAME = "data";

  private final String qualifier;
  private final String subject;
  private final Object data;

  //// builders

  public static Builder from(StreamMessage message) {
    return builder()
        .qualifier(message.qualifier)
        .subject(message.subject)
        .data(message.data);
  }

  public static Builder builder() {
    return new Builder();
  }

  private StreamMessage(Builder builder) {
    this.qualifier = builder.qualifier;
    this.subject = builder.subject;
    this.data = builder.data;
  }

  //// accessors

  public String qualifier() {
    return qualifier;
  }

  public String subject() {
    return subject;
  }

  @SuppressWarnings("unchecked")
  public <T> T data() {
    return (T) data;
  }

  public boolean containsData() {
    return data != null;
  }

  public boolean containsSubject() {
    return subject != null && !subject.isEmpty();
  }

  public boolean dataOfType(Class<?> clazz) {
    return clazz.isInstance(data);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    StreamMessage message = (StreamMessage) obj;
    return Objects.equals(qualifier, message.qualifier)
        && Objects.equals(data, message.data)
        && Objects.equals(subject, message.subject);
  }

  @Override
  public int hashCode() {
    return Objects.hash(qualifier, subject, data);
  }

  @Override
  public String toString() {
    return "StreamMessage [q=" + qualifier
        + ", subject=" + subject
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
    private String subject;
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

    public Builder qualifier(String serviceName, String methodMame) {
      return qualifier(Qualifier.fromString(serviceName + "/" + methodMame));
    }
    
    public Builder subject(String subject) {
      this.subject = subject;
      return this;
    }

    public Builder data(Object data) {
      this.data = data;
      return this;
    }

    public StreamMessage build() {
      return new StreamMessage(this);
    }

    
  }
}
