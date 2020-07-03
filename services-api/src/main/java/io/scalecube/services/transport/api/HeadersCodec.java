package io.scalecube.services.transport.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public interface HeadersCodec {

  HeadersCodec DEFAULT_INSTANCE = new JdkCodec();

  void encode(OutputStream stream, Map<String, String> headers) throws IOException;

  Map<String, String> decode(InputStream stream) throws IOException;
}
