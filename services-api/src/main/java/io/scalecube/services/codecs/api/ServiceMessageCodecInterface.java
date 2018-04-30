package io.scalecube.services.codecs.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public interface ServiceMessageCodecInterface {

  String contentType();

  void writeHeaders(OutputStream stream, Map<String, String> headers) throws IOException;

  Map<String, String> readHeaders(InputStream stream) throws IOException;

  void writeBody(OutputStream stream, Object value) throws IOException;

  Object readBody(InputStream stream, Class<?> type) throws IOException;

}
