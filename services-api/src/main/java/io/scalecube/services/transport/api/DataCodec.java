package io.scalecube.services.transport.api;

import io.scalecube.utils.ServiceLoaderUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Data code service provider interface. */
public interface DataCodec {

  Map<String, DataCodec> INSTANCES =
      ServiceLoaderUtil.findAll(DataCodec.class)
          .collect(Collectors.toMap(DataCodec::contentType, Function.identity()));

  static Collection<DataCodec> getAllInstances() {
    return INSTANCES.values();
  }

  static Set<String> getAllContentTypes() {
    return getAllInstances().stream().map(DataCodec::contentType).collect(Collectors.toSet());
  }

  /**
   * Get a DataCodec for a content type.
   *
   * @param contentType the content type.
   * @return a DataCodec for the content type or IllegalArgumentException is thrown if non exist
   */
  static DataCodec getInstance(String contentType) {
    if (contentType == null) {
      throw new IllegalArgumentException("contentType not specified");
    }
    DataCodec dataCodec = INSTANCES.get(contentType);
    if (dataCodec == null) {
      throw new IllegalArgumentException("DataCodec for '" + contentType + "' not configured");
    }
    return dataCodec;
  }

  String contentType();

  void encode(OutputStream stream, Object value) throws IOException;

  Object decode(InputStream stream, Type type) throws IOException;
}
