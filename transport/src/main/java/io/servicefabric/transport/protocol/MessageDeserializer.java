package io.servicefabric.transport.protocol;

import io.netty.buffer.ByteBuf;

public interface MessageDeserializer {

	Message deserialize(ByteBuf bb);

}
