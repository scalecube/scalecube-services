package io.scalecube.services.transport.protostuff;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.StringMapSchema;
import io.protostuff.runtime.RuntimeSchema;
import io.scalecube.services.exceptions.MessageCodecException;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public final class ProtostuffCodec implements HeadersCodec, DataCodec {

  public static final String CONTENT_TYPE = "application/protostuff";

  @Override
  public String contentType() {
    return CONTENT_TYPE;
  }

  @Override
  public void encode(OutputStream stream, Object value) throws IOException {
    //noinspection rawtypes
    Schema schema = RuntimeSchema.getSchema(value.getClass());
    //noinspection unchecked
    ProtobufIOUtil.writeTo(stream, value, schema, LinkedBuffer.allocate());
  }

  @Override
  public void encode(OutputStream stream, Map<String, String> headers) throws IOException {
    ProtostuffIOUtil.writeTo(
        stream, headers, StringMapSchema.VALUE_STRING, LinkedBuffer.allocate());
  }

  @Override
  public Object decode(InputStream stream, Type type) throws IOException {
    try {
      Class<?> clazz = null;
      if (type instanceof Class<?>) {
        clazz = (Class<?>) type;
      } else if (type instanceof ParameterizedType) {
        clazz = Class.forName(((ParameterizedType) type).getRawType().getTypeName());
      }
      //noinspection rawtypes
      Schema schema = RuntimeSchema.getSchema(clazz);
      Object result = schema.newMessage();
      //noinspection unchecked
      ProtobufIOUtil.mergeFrom(stream, result, schema, LinkedBuffer.allocate());
      return result;
    } catch (ClassNotFoundException e) {
      throw new MessageCodecException("Couldn't decode message", e);
    }
  }

  @Override
  public Map<String, String> decode(InputStream stream) throws IOException {
    HashMap<String, String> map = new HashMap<>();
    ProtostuffIOUtil.mergeFrom(stream, map, StringMapSchema.VALUE_STRING, LinkedBuffer.allocate());
    return map;
  }
}
