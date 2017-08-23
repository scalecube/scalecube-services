package io.scalecube.transport;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * The Class Message introduces generic protocol used for point to point communication by transport.
 */
public final class Message {

  /**
   * This header is supposed to be used by application in case if same data type can be reused for several messages
   * so it will allow to qualify the specific message type.
   */
  public static final String HEADER_QUALIFIER = "q";

  /**
   * This header is supposed to be used by application in order to correlate request and response messages.
   */
  public static final String HEADER_CORRELATION_ID = "cid";

  /**
   * This is a system header which used by transport for serialization and deserialization purpose. It is not supposed
   * to be used by application directly and it is subject to changes in future releases.
   */
  public static final String HEADER_DATA_TYPE = "_type";

  private Map<String, String> headers = Collections.emptyMap();
  private Object data;
  private Address sender;

  /**
   * Instantiates empty message for deserialization purpose.
   */
  Message() {}

  private Message(Builder builder) {
    this.data = builder.data;
    this.headers = builder.headers;
  }

  /**
   * Instantiates a new message with the given data and without headers.
   */
  public static Message fromData(Object data) {
    return withData(data).build();
  }

  /**
   * Instantiates a new message builder with the given data and without headers.
   */
  public static Builder withData(Object data) {
    return builder().data(data);
  }


  /**
   * Instantiates a new message with the given headers and with empty data.
   */
  public static Message fromHeaders(Map<String, String> headers) {
    return withHeaders(headers).build();
  }

  /**
   * Instantiates a new message builder with the given headers and with empty data.
   */
  public static Builder withHeaders(Map<String, String> headers) {
    return builder().headers(headers);
  }

  /**
   * Instantiates a new message with the given qualifier header and with empty data.
   */
  public static Message fromQualifier(String qualifier) {
    return withQualifier(qualifier).build();
  }

  /**
   * Instantiates a new message builder with the given qualifier header and with empty data.
   */
  public static Builder withQualifier(String qualifier) {
    return builder().qualifier(qualifier);
  }

  /**
   * Instantiates new message with the same data and headers as at given message.
   */
  public static Message from(Message message) {
    return with(message).build();
  }

  /**
   * Instantiates new message builder with the same data and headers as at given message.
   */
  public static Builder with(Message message) {
    return withData(message.data).headers(message.headers);
  }

  /**
   * Instantiates new empty message builder.
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
    checkArgument(headers != null);
    this.headers = Collections.unmodifiableMap(headers);
  }

  /**
   * Sets sender and used by transport send method.
   * 
   * @param sender address from where message was sent
   */
  void setSender(Address sender) {
    this.sender = sender;
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
   * Returns the message header by given header name.
   */
  public String header(String name) {
    return headers.get(name);
  }

  public String qualifier() {
    return header(HEADER_QUALIFIER);
  }

  public String correlationId() {
    return header(HEADER_CORRELATION_ID);
  }

  /**
   * Return the message data, which can be byte array, string or any type.
   * 
   * @return payload of the message or null if message is without any payload
   */
  @SuppressWarnings("unchecked")
  public <T> T data() {
    return (T) data;
  }

  public Address sender() {
    return sender;
  }

  @Override
  public String toString() {
    return "Message {headers: " + headers + ", sender: " + sender + ", data: " + data + '}';
  }

  public static class Builder {

    private Map<String, String> headers = new HashMap<>();
    private Object data;

    private Builder() {
    }

    public Builder data(Object data) {
      this.data = data;
      return this;
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

    public Builder correlationId(String correlationId) {
      return header(HEADER_CORRELATION_ID, correlationId);
    }

    public Message build() {
      return new Message(this);
    }
  }
}
