package io.servicefabric.transport.protocol;

import io.netty.buffer.ByteBuf;

public interface MessageSerializer {

	void serialize(Message message, ByteBuf bb);
}
