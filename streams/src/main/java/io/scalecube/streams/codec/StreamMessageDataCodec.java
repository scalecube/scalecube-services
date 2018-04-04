package io.scalecube.streams.codec;

import io.scalecube.streams.StreamMessage;

import java.io.IOException;
import java.lang.reflect.Type;

public interface StreamMessageDataCodec {

  StreamMessage decodeData(StreamMessage message, Type type);

  StreamMessage encodeData(StreamMessage message);
}
