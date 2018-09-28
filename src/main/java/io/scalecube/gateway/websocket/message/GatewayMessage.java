package io.scalecube.gateway.websocket.message;

import io.netty.buffer.ByteBuf;
import io.scalecube.services.api.ServiceMessage;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GatewayMessage {

  public static final String QUALIFIER_FIELD = "q";
  public static final String STREAM_ID_FIELD = "sid";
  public static final String SIGNAL_FIELD = "sig";
  public static final String DATA_FIELD = "d";
  public static final String INACTIVITY_FIELD = "i";

  private final Map<String, String> headers;
  private final Object data;

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Get a builder by pattern form given {@link GatewayMessage}.
   *
   * @param message Message form where to copy field values.
   * @return builder with fields copied from given {@link GatewayMessage}
   */
  public static Builder from(GatewayMessage message) {
    return new Builder().headers(message.headers).data(message.data);
  }

  /**
   * Get a builder by pattern form given {@link ServiceMessage}.
   *
   * @param message ServiceMessage form where to copy field values.
   * @return builder with fields copied from given {@link ServiceMessage}
   */
  public static Builder from(ServiceMessage message) {
    return new Builder().headers(message.headers()).data(message.data());
  }

  private GatewayMessage(Builder builder) {
    this.data = builder.data;
    this.headers = Collections.unmodifiableMap(new HashMap<>(builder.headers));
  }

  /**
   * {@link GatewayMessage} to {@link ServiceMessage} converter.
   *
   * @param message gateway message
   * @return service message
   */
  public static ServiceMessage toServiceMessage(GatewayMessage message) {
    return ServiceMessage.builder().headers(message.headers).data(message.data).build();
  }

  public String qualifier() {
    return headers.get(QUALIFIER_FIELD);
  }

  public Long streamId() {
    String value = headers.get(STREAM_ID_FIELD);
    return value != null ? Long.parseLong(value) : null;
  }

  public Integer signal() {
    String value = headers.get(SIGNAL_FIELD);
    return value != null ? Integer.valueOf(value) : null;
  }

  public <T> T data() {
    // noinspection unchecked
    return (T) data;
  }

  public Integer inactivity() {
    String value = headers.get(INACTIVITY_FIELD);
    return value != null ? Integer.valueOf(value) : null;
  }

  public boolean hasSignal(Signal signal) {
    String value = headers.get(SIGNAL_FIELD);
    return value != null && Integer.parseInt(value) == signal.code();
  }

  public Map<String, String> headers() {
    return headers;
  }

  public boolean hasHeader(String name) {
    return headers.containsKey(name);
  }

  @Override
  public String toString() {
    return "GatewayMessage {headers: " + headers + ", data: " + dataToString() + '}';
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

    Builder() {}

    public Builder qualifier(String qualifier) {
      return header(QUALIFIER_FIELD, qualifier);
    }

    public Builder streamId(Long streamId) {
      return header(STREAM_ID_FIELD, streamId);
    }

    public Builder signal(Integer signal) {
      return header(SIGNAL_FIELD, signal);
    }

    public Builder signal(Signal signal) {
      return signal(signal.code());
    }

    public Builder inactivity(Integer inactivity) {
      return header(INACTIVITY_FIELD, inactivity);
    }

    public Builder data(Object data) {
      this.data = data;
      return this;
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

    /**
     * Add all headers.
     *
     * @param headers given headers
     * @return self
     */
    public Builder headers(Map<String, String> headers) {
      headers.forEach(this::header);
      return this;
    }

    /**
     * Finally build the {@link GatewayMessage} from current builder.
     *
     * @return {@link GatewayMessage} with parameters from current builder.
     */
    public GatewayMessage build() {
      return new GatewayMessage(this);
    }
  }
}
