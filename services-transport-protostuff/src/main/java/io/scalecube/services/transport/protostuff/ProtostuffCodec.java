package io.scalecube.services.transport.protostuff;

import io.protostuff.ProtobufIOUtil;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.StringMapSchema;
import io.protostuff.runtime.RuntimeSchema;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public final class ProtostuffCodec implements HeadersCodec, DataCodec {

  public static final String CONTENT_TYPE = "application/protostuff";

  private static final RecyclableLinkedBuffer recyclableLinkedBuffer = new RecyclableLinkedBuffer();

  @Override
  public String contentType() {
    return CONTENT_TYPE;
  }

  @Override
  public void encode(OutputStream stream, Object value) throws IOException {
    Schema schema = RuntimeSchema.getSchema(value.getClass());
    try (RecyclableLinkedBuffer rlb = recyclableLinkedBuffer.get()) {
      ProtobufIOUtil.writeTo(stream, value, schema, rlb.buffer());
    }
  }

  @Override
  public void encode(OutputStream stream, Map<String, String> headers) throws IOException {
    try (RecyclableLinkedBuffer rlb = recyclableLinkedBuffer.get()) {
      ProtostuffIOUtil.writeTo(stream, headers, StringMapSchema.VALUE_STRING, rlb.buffer());
    }
  }

  @Override
  public Object decode(InputStream stream, Class<?> type) throws IOException {
    Schema schema = RuntimeSchema.getSchema(type);
    Object result = schema.newMessage();

    try (RecyclableLinkedBuffer rlb = recyclableLinkedBuffer.get()) {
      ProtobufIOUtil.mergeFrom(stream, result, schema, rlb.buffer());
    }
    return result;
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
