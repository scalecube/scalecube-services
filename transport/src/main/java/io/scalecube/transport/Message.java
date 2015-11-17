package io.scalecube.transport;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * The Class Message introduces generic protocol used for point to point communication by transport.
 */
public final class Message {

  private Map<String, String> headers = Collections.emptyMap();
  private Object data;
  private TransportEndpoint sender;

  /**
   * Instantiates empty message for deserialization purpose.
   */
  Message() {}

  /**
   * Instantiates a new message with the given data and without headers.
   */
  public Message(Object data) {
    setData(data);
  }

  /**
   * Instantiates a new message with the given headers and with empty data.
   */
  public Message(Map<String, String> headers) {
    setHeaders(headers);
  }

  /**
   * Instantiates new messages with given qualifier, data and correlationId.
   */
  public Message(Object data, Map<String, String> headers) {
    setData(data);
    setHeaders(headers);
  }

  /**
   * Instantiates new message with the given data and headers. Headers passed a sequence of key-value pairs.
   */
  public Message(Object data, String... headers) {
    checkArgument(headers != null);
    checkArgument(headers.length % 2 == 0, "");
    Map<String, String> headersMap = new HashMap<>(headers.length / 2);
    for (int i = 0; i < headers.length; i += 2) {
      headersMap.put(headers[i], headers[i + 1]);
    }
    this.headers = Collections.unmodifiableMap(headersMap);
    setData(data);
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
   * Sets sender and used by transport pipeline. Actual sender not passed via network in the message, but resolved on
   * the receiveing side.
   * 
   * @param sender endpoint from where message was received
   */
  void setSender(TransportEndpoint sender) {
    checkArgument(sender != null);
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
  public String header(String headerName) {
    return headers.get(headerName);
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

  public TransportEndpoint sender() {
    return sender;
  }

  @Override
  public String toString() {
    return "Message{"
        + "headers=" + headers
        + ", data=" + (data == null ? "null" : data.getClass().getSimpleName())
        + '}';
  }
}
