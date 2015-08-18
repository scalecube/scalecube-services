package io.servicefabric.transport.protocol.protostuff;

import static io.protostuff.ProtostuffIOUtil.toByteArray;
import static io.protostuff.ProtostuffIOUtil.writeTo;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.handler.codec.EncoderException;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import io.servicefabric.transport.protocol.Message;
import io.servicefabric.transport.protocol.MessageSerializer;
import io.servicefabric.transport.utils.RecycleableLinkedBuffer;

public final class ProtostuffMessageSerializer implements MessageSerializer {

	private static final RecycleableLinkedBuffer recycleableLinkedBuffer = new RecycleableLinkedBuffer();

	@Override
	public void serialize(Message message, ByteBuf bb) {
		BinaryMessage binaryMessage = new BinaryMessage();
		binaryMessage.setQualifier(message.qualifier());
		binaryMessage.setCorrelationId(message.correlationId());
		binaryMessage.setData(serializeData(message.data()));
		Schema<BinaryMessage> schema = RuntimeSchema.getSchema(BinaryMessage.class);
		try (RecycleableLinkedBuffer rlb = recycleableLinkedBuffer.get()) {
			try {
				writeTo(new ByteBufOutputStream(bb), binaryMessage, schema, rlb.buffer());
			} catch (Exception e) {
				throw new EncoderException(e.getMessage(), e);
			}
		}
	}

	private byte[] serializeData(Object data) {
		if (data == null) {
			return null;
		} else if (data instanceof byte[]) {
			return (byte[]) data;
		}
		Schema schema = RuntimeSchema.getSchema(data.getClass());
		try (RecycleableLinkedBuffer rlb = recycleableLinkedBuffer.get()) {
			return toByteArray(data, schema, rlb.buffer());
		}
	}
}
