package io.scalecube.services.codec.protostuff;

import io.scalecube.services.codec.HeadersCodec;

import io.protostuff.ProtostuffIOUtil;
import io.protostuff.StringMapSchema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public class ProtostuffHeadersCodec implements HeadersCodec {

  private static final RecyclableLinkedBuffer recyclableLinkedBuffer = new RecyclableLinkedBuffer();

  @Override
  public String contentType() {
    return "application/protostuff";
  }

  @Override
  public void encode(OutputStream stream, Map<String, String> headers) throws IOException {
    try (RecyclableLinkedBuffer rlb = recyclableLinkedBuffer.get()) {
      ProtostuffIOUtil.writeTo(stream, headers, StringMapSchema.VALUE_STRING, rlb.buffer());
    }
  }

  @Override
  public Map<String, String> decode(InputStream stream) throws IOException {
    HashMap<String, String> map = new HashMap<>();
    try (RecyclableLinkedBuffer rlb = recyclableLinkedBuffer.get()) {
      ProtostuffIOUtil.mergeFrom(stream, map, StringMapSchema.VALUE_STRING, rlb.buffer());
    }
    return map;
  }
}
