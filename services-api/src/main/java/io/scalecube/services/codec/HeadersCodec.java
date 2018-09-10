package io.scalecube.services.codec;

import io.scalecube.services.ServiceLoaderUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Headers code service provider interface. */
public interface HeadersCodec {

  Map<String, HeadersCodec> INSTANCES = new ConcurrentHashMap<>();

  static HeadersCodec getInstance(String contentType) {
    return INSTANCES.computeIfAbsent(contentType, HeadersCodec::loadInstance);
  }

  /**
   * Get a HeadersCodec for a content type.
   *
   * @param contentType the content type.
   * @return a Headers codec for the content type or IllegalArgumentException is thrown if non exist
   */
  static HeadersCodec loadInstance(String contentType) {
    return ServiceLoaderUtil.findFirst(
            HeadersCodec.class, codec -> codec.contentType().equalsIgnoreCase(contentType))
        .orElseThrow(() -> new IllegalArgumentException("HeadersCodec not configured"));
  }

  String contentType();

  void encode(OutputStream stream, Map<String, String> headers) throws IOException;

  Map<String, String> decode(InputStream stream) throws IOException;
}
