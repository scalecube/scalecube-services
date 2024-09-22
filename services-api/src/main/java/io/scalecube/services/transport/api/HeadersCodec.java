package io.scalecube.services.transport.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.StreamSupport;

public interface HeadersCodec {

  HeadersCodec DEFAULT_INSTANCE = new JdkCodec();

  Map<String, HeadersCodec> INSTANCES = new ConcurrentHashMap<>();

  static HeadersCodec getInstance(String contentType) {
    return INSTANCES.computeIfAbsent(contentType, HeadersCodec::loadInstance);
  }

  /**
   * Returns {@link HeadersCodec} by given {@code contentType}.
   *
   * @param contentType contentType (required)
   * @return {@link HeadersCodec} by given {@code contentType} (or throws IllegalArgumentException
   *     is thrown if not exist)
   */
  static HeadersCodec loadInstance(String contentType) {
    return StreamSupport.stream(ServiceLoader.load(HeadersCodec.class).spliterator(), false)
        .filter(codec -> codec.contentType().equalsIgnoreCase(contentType))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "HeadersCodec for '" + contentType + "' not configured"));
  }

  String contentType();

  void encode(OutputStream stream, Map<String, String> headers) throws IOException;

  Map<String, String> decode(InputStream stream) throws IOException;
}
