package io.scalecube.transport;

import io.netty.buffer.ByteBuf;

public interface MessageDeserializer {

  Message deserialize(ByteBuf bb);
}
