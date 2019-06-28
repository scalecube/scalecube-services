package io.scalecube.services.transport.api;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Simple binary codec for headers service message.
 */
public class BinaryHeadersCodec implements HeadersCodec {

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
    ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
    try {
      buffer.writeInt(headers.size());
      for (Entry<String, String> header : headers.entrySet()) {
        byte[] nameBytes = header.getKey().getBytes(UTF_8);
        buffer.writeInt(nameBytes.length);
        buffer.writeBytes(nameBytes);
        byte[] valueBytes = header.getValue().getBytes(UTF_8);
        buffer.writeInt(valueBytes.length);
        buffer.writeBytes(valueBytes);
      }

      if (stream instanceof ByteBufOutputStream) {
        ((ByteBufOutputStream) stream).buffer().writeBytes(buffer);
      } else if (buffer.hasArray()) {
        byte[] array = buffer.array();
        stream.write(array);
      } else {
        int readableBytes = buffer.readableBytes();
        byte[] bytes = new byte[readableBytes];
        buffer.readBytes(bytes, 0, readableBytes);
        stream.write(bytes);
      }
    } finally {
      buffer.release();
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
    ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer(stream.available());
    try {
      buffer.writeBytes(stream, stream.available());
      int size = buffer.readInt();
      Map<String, String> headers = new HashMap<>(size);
      for (int i = 0; i < size; i++) {
        int nameLength = buffer.readInt();
        CharSequence name = buffer.readCharSequence(nameLength, UTF_8);
        int valueLength = buffer.readInt();
        CharSequence value = buffer.readCharSequence(valueLength, UTF_8);
        headers.put(name.toString(), value.toString());
      }
      return headers;
    } finally {
      buffer.release();
    }
  }
}
