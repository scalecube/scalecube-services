package io.scalecube.services.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

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
   * Instantiates new message with error qualifier for given error type and specified error code and
   * message.
   *
   * @param errorType the error type to be used in message qualifier.
   * @param errorCode the error code.
   * @param errorMessage the error message.
   * @return builder.
   */
  public static ServiceMessage error(int errorType, int errorCode, String errorMessage) {
    return ServiceMessage.builder()
        .qualifier(Qualifier.asError(errorType))
        .data(new ErrorData(errorCode, errorMessage))
        .build();
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
    Objects.requireNonNull(name, "header name");
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

  /**
   * Returns data format of the message data or default one.
   *
   * @return data format of the data or default one
   */
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
   * @param dataClass the expected class of the data
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

  /**
   * Describes whether the message is an error.
   *
   * @return <code>true</code> if error, otherwise <code>false</code>.
   */
  public boolean isError() {
    String qualifier = qualifier();
    if (qualifier == null) {
      throw new IllegalStateException("Message doesn't have qualifier");
    }
    return Qualifier.getQualifierNamespace(qualifier).equals(Qualifier.ERROR_NAMESPACE);
  }

  /**
   * Returns error type. Error type is an identifier of a group of errors.
   *
   * @return error type.
   */
  public int errorType() {
    if (!isError()) {
      throw new IllegalStateException("Message is not an error");
    }

    try {
      return Integer.parseInt(Qualifier.getQualifierAction(qualifier()));
    } catch (NumberFormatException e) {
      throw new IllegalStateException("Error type must be a number");
    }
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ServiceMessage.class.getSimpleName() + "[", "]")
        .add("headers(" + headers.size() + ")")
        .add("data=" + (data != null ? data.getClass().getName() : null))
        .toString();
  }

  public static class Builder {

    private Map<String, String> headers = new HashMap<>();
    private Object data;

    private Builder() {}

    /**
     * Setter for {@code data}.
     *
     * @param data data; optional
     * @return this builder
     */
    public Builder data(Object data) {
      this.data = data;
      return this;
    }

    /**
     * Setter for {@code dataType}.
     *
     * @deprecated in future releases will be dropped without replacement
     * @param dataType data type; no null
     * @return this builder
     */
    @Deprecated
    public Builder dataType(Class<?> dataType) {
      Objects.requireNonNull(dataType, "dataType");
      headers.put(HEADER_DATA_TYPE, dataType.getName());
      return this;
    }

    /**
     * Setter for {@code dataFormat}.
     *
     * @param dataFormat data format; not null
     * @return this builder
     */
    public Builder dataFormat(String dataFormat) {
      Objects.requireNonNull(dataFormat, "dataFormat");
      headers.put(HEADER_DATA_FORMAT, dataFormat);
      return this;
    }

    /**
     * Returns {@code headers}.
     *
     * @return headers
     */
    private Map<String, String> headers() {
      return headers;
    }

    /**
     * Setter for {@code headers}.
     *
     * @param headers headers; not null
     * @return this builder
     */
    public Builder headers(Map<String, String> headers) {
      Objects.requireNonNull(headers, "headers").forEach(this::header);
      return this;
    }

    /**
     * Setter for header key-value pair.
     *
     * @param key header name; not null
     * @param value header value; not null
     * @return this builder
     */
    public Builder header(String key, String value) {
      Objects.requireNonNull(key, "header name");
      Objects.requireNonNull(value, "header value");
      headers.put(key, value);
      return this;
    }

    /**
     * Setter for header key-value pair.
     *
     * @param key header name; not null
     * @param value header value; not null
     * @return this builder
     */
    public Builder header(String key, Object value) {
      Objects.requireNonNull(key, "header name");
      Objects.requireNonNull(value, "header value");
      headers.put(key, value.toString());
      return this;
    }

    /**
     * Setter for {@code qualifier}.
     *
     * @param qualifier qualifier; not null
     * @return this builder
     */
    public Builder qualifier(String qualifier) {
      return header(HEADER_QUALIFIER, Objects.requireNonNull(qualifier, "qualifier"));
    }

    /**
     * Setter for {@code qualifier}.
     *
     * @param namespace namespace; not null
     * @param action action; not null
     * @return this builder
     */
    public Builder qualifier(String namespace, String action) {
      return qualifier(
          Qualifier.asString(
              Objects.requireNonNull(namespace, "namespace"),
              Objects.requireNonNull(action, "action")));
    }

    public ServiceMessage build() {
      return new ServiceMessage(this);
    }
  }
}
