package io.scalecube.streams.codec;

import io.scalecube.streams.StreamMessage;

public interface StreamMessageDataCodec {

  StreamMessage decodeData(StreamMessage message, Class type);

  StreamMessage encodeData(StreamMessage message);
}
