package io.servicefabric.transport;

import io.netty.buffer.ByteBuf;

public interface MessageDeserializer {

  Message deserialize(ByteBuf bb);

}
