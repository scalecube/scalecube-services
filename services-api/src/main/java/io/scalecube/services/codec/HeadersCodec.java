package io.scalecube.services.codec;

import io.scalecube.services.ServiceLoaderUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public interface HeadersCodec {

  Map<String, HeadersCodec> INSTANCES = new ConcurrentHashMap<>();

  static HeadersCodec getInstance(String contentType) {
    return INSTANCES.computeIfAbsent(contentType, HeadersCodec::loadInstance);
  }

  static HeadersCodec loadInstance(String contentType) {
    Optional<HeadersCodec> result = ServiceLoaderUtil.findFirst(HeadersCodec.class,
        codec -> codec.contentType().equalsIgnoreCase(contentType));
    return result.orElseThrow(() -> new IllegalStateException("HeadersCodec not configured"));
  }

  String contentType();

  void encode(OutputStream stream, Map<String, String> headers) throws IOException;

  Map<String, String> decode(InputStream stream) throws IOException;

}
