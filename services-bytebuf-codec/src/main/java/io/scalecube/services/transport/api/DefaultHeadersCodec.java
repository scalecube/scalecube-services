package io.scalecube.services.transport.api;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/** Simple binary codec for headers service message. */
public final class DefaultHeadersCodec implements HeadersCodec {

  /**
   * {@inheritDoc}
   */
  @Override
  public String contentType() {
    return "application/octet-stream";
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void encode(OutputStream stream, Map<String, String> headers) throws IOException {
    if (headers.isEmpty()) {
      return;
    }
    writeInt(stream, headers.size());
    for (Entry<String, String> header : headers.entrySet()) {
      byte[] nameBytes = header.getKey().getBytes(UTF_8);
      writeInt(stream, nameBytes.length);
      stream.write(nameBytes);
      byte[] valueBytes = header.getValue().getBytes(UTF_8);
      writeInt(stream, valueBytes.length);
      stream.write(valueBytes);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, String> decode(InputStream stream) throws IOException {
    if (stream.available() < 1) {
      return Collections.emptyMap();
    }
    int size = readInt(stream);
    Map<String, String> headers = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      int nameLength = readInt(stream);
      byte[] nameBytes = new byte[nameLength];
      stream.read(nameBytes);
      String name = new String(nameBytes, UTF_8);
      int valueLength = readInt(stream);
      byte[] valueBytes = new byte[valueLength];
      stream.read(valueBytes);
      String value = new String(valueBytes, UTF_8);
      headers.put(name, value);
    }
    return headers;
  }

  private void writeInt(OutputStream stream, int number) throws IOException {
    for (int i = Integer.BYTES - 1; i >= 0; i--) {
      stream.write(number >>> i * Byte.SIZE);
    }
  }

  private Integer readInt(InputStream stream) throws IOException {
    int r = 0;
    for (int i = Integer.BYTES - 1; i >= 0; i--) {
      r = r | ((stream.read() & 0xFF) << i * Byte.SIZE);
    }
    return r;
  }
}
