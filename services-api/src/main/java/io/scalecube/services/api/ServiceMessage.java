package io.scalecube.services.api;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class ServiceMessage {

  /**
   * This header is supposed to be used by application in case if same data type can be reused for several messages so
   * it will allow to qualify the specific message type.
   */
  static final String HEADER_QUALIFIER = "q";


  /**
   * This header stands for "Stream Id" and has to be used for Stream multiplexing. Messages within one logical stream
   * have to be signed with equal sid-s.
   */
  static final String HEADER_STREAM_ID = "sid";

  /**
   * This is a system header which used by transport for serialization and deserialization purpose. It is not supposed
   * to be used by application directly and it is subject to changes in future releases.
   */
  static final String HEADER_DATA_TYPE = "_type";
  static final String HEADER_DATA_FORMAT = "_data_format";

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
   * Returns message's qualifier.
   * 
   * @return qualifier string
   */
  public String qualifier() {
    return header(HEADER_QUALIFIER);
  }

  /**
   * Returns message's sid.
   *
   * @return streamId.
   */
  public String streamId() {
    return header(HEADER_STREAM_ID);
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

    public Builder dataFormat(String dataFormat) {
      headers.put(HEADER_DATA_FORMAT, dataFormat);
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

    public Builder streamId(String streamId) {
      return header(HEADER_STREAM_ID, streamId);
    }

    public ServiceMessage build() {
      return new ServiceMessage(this);
    }

    public Builder qualifier(String serviceName, String methodName) {
      return qualifier(Qualifier.asString(serviceName, methodName));
    }
  }
}
