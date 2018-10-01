package io.scalecube.services.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class ServiceMessage {

  /** Default message data content type. */
  static final String DEFAULT_DATA_FORMAT = "application/json";

  /**
   * This header is supposed to be used by application in case if same data type can be reused for
   * several messages so it will allow to qualify the specific message type.
   */
  static final String HEADER_QUALIFIER = "q";

  /**
   * This is a system header which used by transport for serialization and deserialization purpose.
   * It is not supposed to be used by application directly and it is subject to changes in future
   * releases.
   */
  static final String HEADER_DATA_TYPE = "_type";

  /**
   * Data format header. Json, Protostuff and etc. Note that default data format is defined at
   * {@link #DEFAULT_DATA_FORMAT}.
   */
  public static final String HEADER_DATA_FORMAT = "_data_format";

  private Map<String, String> headers = new HashMap<>(1);
  private Object data;

  /** Instantiates empty message for deserialization purpose. */
  ServiceMessage() {}

  private ServiceMessage(Builder builder) {
    this.data = builder.data;
    this.headers = Collections.unmodifiableMap(new HashMap<>(builder.headers));
  }

  /**
   * Instantiates new message with the same data and headers as at given message.
   *
   * @param message the message to be copied
   * @return a new message, with the same data and headers
   */
  public static Builder from(ServiceMessage message) {
    return ServiceMessage.builder().data(message.data()).headers(message.headers());
  }

  /**
   * Instantiates new empty message builder.
   *
   * @return new builder
   */
  public static Builder builder() {
    return new Builder();
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
    this.headers = Collections.unmodifiableMap(new HashMap<>(headers));
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
    Objects.requireNonNull(name);
    return headers.get(name);
  }

  /**
   * Returns message's qualifier.
   *
   * @return qualifier string
   */
  public String qualifier() {
    return header(HEADER_QUALIFIER);
  }

  /**
   * Returns data format of the message data.
   *
   * @return data format of the data
   */
  public String dataFormat() {
    return header(HEADER_DATA_FORMAT);
  }

  public String dataFormatOrDefault() {
    return Optional.ofNullable(dataFormat()).orElse(DEFAULT_DATA_FORMAT);
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

  public boolean hasData() {
    return data != null;
  }

  /**
   * Verify that this message contains data.
   *
   * @param dataClass the expected class of the dara
   * @return true if the data is instance of the dataClass
   */
  public boolean hasData(Class<?> dataClass) {
    if (dataClass == null) {
      return false;
    }
    if (dataClass.isPrimitive()) {
      return hasData();
    } else {
      return dataClass.isInstance(data);
    }
  }

  @Override
  public String toString() {
    return "ServiceMessage {headers: " + headers + ", data: " + data + '}';
  }

  public static class Builder {

    private Map<String, String> headers = new HashMap<>();
    private Object data;

    private Builder() {}

    public Builder data(Object data) {
      this.data = data;
      return this;
    }

    public Builder dataType(Class<?> data) {
      headers.put(HEADER_DATA_TYPE, data.getName());
      return this;
    }

    public Builder dataFormat(String dataFormat) {
      headers.put(HEADER_DATA_FORMAT, dataFormat);
      return this;
    }

    private Map<String, String> headers() {
      return this.headers;
    }

    public Builder headers(Map<String, String> headers) {
      headers.forEach(this::header);
      return this;
    }

    /**
     * Sets a header key value pair.
     *
     * @param key key; not null
     * @param value value; not null
     * @return self
     */
    public Builder header(String key, String value) {
      Objects.requireNonNull(key);
      Objects.requireNonNull(value);
      headers.put(key, value);
      return this;
    }

    /**
     * Sets a header key value pair.
     *
     * @param key key; not null
     * @param value value; not null
     * @return self
     */
    public Builder header(String key, Object value) {
      Objects.requireNonNull(key);
      Objects.requireNonNull(value);
      headers.put(key, value.toString());
      return this;
    }

    public Builder qualifier(String qualifier) {
      return header(HEADER_QUALIFIER, qualifier);
    }

    public Builder qualifier(String serviceName, String methodName) {
      return qualifier(Qualifier.asString(serviceName, methodName));
    }

    public ServiceMessage build() {
      return new ServiceMessage(this);
    }
  }
}
