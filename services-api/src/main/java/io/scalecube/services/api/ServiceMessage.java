package io.scalecube.services.api;


import static java.util.Objects.requireNonNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class ServiceMessage {

  /**
   * This header is supposed to be used by application in case if same data type can be reused for several messages so
   * it will allow to qualify the specific message type.
   */
  public static final String HEADER_QUALIFIER = "q";

  /**
   * This is a system header which used by transport for serialization and deserialization purpose. It is not supposed
   * to be used by application directly and it is subject to changes in future releases.
   */
  public static final String HEADER_DATA_TYPE = "_type";
  public static final String HEADER_RESPONSE_DATA_TYPE = "_response_type";

  private Map<String, String> headers = Collections.emptyMap();
  private Object data;

  /**
   * Instantiates empty message for deserialization purpose.
   */
  ServiceMessage() {}

  private ServiceMessage(Builder builder) {
    this.data = builder.data();
    this.headers = builder.headers();
  }

  /**
   * Instantiates new message with the same data and headers as at given message.
   * 
   * @param message the message to be copied
   * @return a new message, with the same data and headers
   */
  public static Builder from(ServiceMessage message) {
    return ServiceMessage.builder()
        .data(message.data())
        .headers(message.headers());
  }

  /**
   * Instantiates new empty message builder.
   *
   * @return new builder
   */
  public static Builder builder() {
    return Builder.getInstance();
  }

  /**
   * Sets data for deserialization purpose.
   * 
   * @param data data to set
   */
  void setData(Object data) {
    this.data = data;
  }

  /**
   * Sets headers for deserialization purpose.
   * 
   * @param headers headers to set
   */
  void setHeaders(Map<String, String> headers) {
    requireNonNull(headers != null);
    this.headers = Collections.unmodifiableMap(headers);
  }

  /**
   * Returns the message headers.
   *
   * @return message headers
   */
  public Map<String, String> headers() {
    return headers;
  }

  /**
   * Returns header value by given header name.
   * 
   * @param name header name
   * @return the message header by given header name
   */
  public String header(String name) {
    return headers.get(name);
  }

  /**
   * Returns message qualifier.
   * 
   * @return qualifier string
   */
  public String qualifier() {
    return header(HEADER_QUALIFIER);
  }

  /**
   * Returns message qualifier.
   * 
   * @return qualifier string
   */
  public Class<?> responseType() {
    try {
      String typeAsString = header(HEADER_RESPONSE_DATA_TYPE);
      if (typeAsString != null) {
        return Class.forName(typeAsString);
      } else {
        return null;
      }
    } catch (ClassNotFoundException e) {
      return null;
    }
  }

  /**
   * Return the message data, which can be byte array, string or any type.
   *
   * @param <T> data type
   * @return payload of the message or null if message is without any payload
   */
  public <T> T data() {
    // noinspection unchecked
    return (T) data;
  }

  @Override
  public String toString() {
    return "ServiceMessage {headers: " + headers + ", data: " + data + '}';
  }

  public static class Builder {

    private Map<String, String> headers = new HashMap<>();
    private Object data;

    private Builder() {}

    static Builder getInstance() {
      return new Builder();
    }

    private Object data() {
      return this.data;
    }

    public Builder data(Object data) {
      this.data = data;
      return this;
    }

    public Builder dataType(Class<?> data) {
      headers.put(HEADER_DATA_TYPE, data.getName());
      return this;
    }

    private Map<String, String> headers() {
      return this.headers;
    }

    public Builder headers(Map<String, String> headers) {
      this.headers.putAll(headers);
      return this;
    }

    public Builder header(String key, String value) {
      headers.put(key, value);
      return this;
    }

    public Builder qualifier(String qualifier) {
      return header(HEADER_QUALIFIER, qualifier);
    }

    public ServiceMessage build() {
      return new ServiceMessage(this);
    }

    public Builder qualifier(String serviceName, String methodName) {
      return this.qualifier(Qualifier.fromString(serviceName + "/" + methodName).asString());
    }
  }
}
