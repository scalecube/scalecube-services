package io.scalecube.transport;

import io.netty.buffer.ByteBuf;

public interface MessageSerializer {

  void serialize(Message message, ByteBuf bb);
}
