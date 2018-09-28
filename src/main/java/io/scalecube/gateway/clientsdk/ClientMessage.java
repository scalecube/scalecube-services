package io.scalecube.gateway.clientsdk;

import io.netty.buffer.ByteBuf;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class ClientMessage {

  private static final String QUALIFIER = "q";
  private Map<String, String> headers;
  private Object data;

  public ClientMessage(Builder builder) {
    this.data = builder.data;
    this.headers = Collections.unmodifiableMap(new HashMap<>(builder.headers));
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder from(ClientMessage message) {
    return new Builder(message);
  }

  public String qualifier() {
    return headers.get(QUALIFIER);
  }

  public Map<String, String> headers() {
    return headers;
  }

  public String header(String name) {
    return headers.get(name);
  }

  public boolean hasHeader(String name) {
    return headers.containsKey(name);
  }

  public <T> T data() {
    // noinspection unchecked
    return (T) data;
  }

  public boolean hasData() {
    return data != null;
  }

  /**
   * Boolean method telling message contains data of given type or not.
   *
   * @param dataClass data class
   * @return true if message has not null data of given type
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
    return "ClientMessage {headers: " + headers + ", data: " + dataToString() + '}';
  }

  private Object dataToString() {
    if (data instanceof ByteBuf) {
      return "bb-" + ((ByteBuf) data).readableBytes();
    }
    if (data instanceof String) {
      return "str-" + ((String) data).length();
    }
    return data;
  }

  public static class Builder {
    private Map<String, String> headers = new HashMap<>(1);
    private Object data;

    private Builder() {}

    public Builder(ClientMessage message) {
      this.data = message.data;
      message.headers().forEach(this::header);
    }

    /**
     * Add a header.
     *
     * @param key header name
     * @param value header value
     * @return self
     */
    public Builder header(String key, String value) {
      if (value != null) {
        headers.put(key, value);
      }
      return this;
    }

    /**
     * Add a header.
     *
     * @param key header name
     * @param value header value
     * @return self
     */
    public Builder header(String key, Object value) {
      if (value != null) {
        headers.put(key, value.toString());
      }
      return this;
    }

    public Builder qualifier(String qualifier) {
      return header(QUALIFIER, qualifier);
    }

    public Builder data(Object data) {
      this.data = data;
      return this;
    }

    public Builder headers(Map<String, String> headers) {
      this.headers.putAll(headers);
      return this;
    }

    public ClientMessage build() {
      return new ClientMessage(this);
    }
  }
}
