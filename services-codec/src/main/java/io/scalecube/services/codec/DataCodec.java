package io.scalecube.services.codec;

import io.scalecube.services.ServiceLoaderUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface DataCodec {

  Map<String, DataCodec> INSTANCES = ServiceLoaderUtil.findAll(DataCodec.class)
      .collect(Collectors.toMap(DataCodec::contentType, Function.identity()));

  static DataCodec getInstance(String contentType) {
    if (contentType == null) {
      throw new IllegalArgumentException("contentType not specified");
    }
    DataCodec dataCodec = INSTANCES.get(contentType);
    if (dataCodec == null) {
      throw new IllegalStateException("DataCodec for '" + contentType + "' not configured");
    }
    return dataCodec;
  }

  static DataCodec getInstance(String contentType, String defaultContentType) {
    if (contentType != null) {
      DataCodec dataCodec = INSTANCES.get(contentType);
      if (dataCodec != null) {
        return dataCodec;
      }
    }
    return getInstance(defaultContentType);
  }

  String contentType();

  void encode(OutputStream stream, Object value) throws IOException;

  Object decode(InputStream stream, Class<?> type) throws IOException;

}
