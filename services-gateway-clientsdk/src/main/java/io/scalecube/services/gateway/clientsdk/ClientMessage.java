package io.scalecube.services.gateway.clientsdk;

import io.netty.buffer.ByteBuf;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class ClientMessage {

  public static final String QUALIFIER_FIELD = "q";
  public static final String RATE_LIMIT_FIELD = "rlimit";

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
    return headers.get(QUALIFIER_FIELD);
  }

  public Integer rateLimit() {
    String value = headers.get(RATE_LIMIT_FIELD);
    return value != null ? Integer.valueOf(value) : null;
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
      Objects.requireNonNull(key);
      Objects.requireNonNull(value);
      headers.put(key, value);
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
      Objects.requireNonNull(key);
      Objects.requireNonNull(value);
      headers.put(key, value.toString());
      return this;
    }

    public Builder qualifier(String qualifier) {
      return header(QUALIFIER_FIELD, qualifier);
    }

    public Builder rateLimit(Integer rateLimit) {
      return header(RATE_LIMIT_FIELD, rateLimit);
    }

    public Builder data(Object data) {
      this.data = data;
      return this;
    }

    public Builder headers(Map<String, String> headers) {
      headers.forEach(this::header);
      return this;
    }

    public ClientMessage build() {
      return new ClientMessage(this);
    }
  }
}
