package io.servicefabric.transport.protocol.protostuff;

import static io.protostuff.ProtostuffIOUtil.mergeFrom;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.DecoderException;
import io.protostuff.Schema;
import io.servicefabric.transport.ITransportTypeRegistry;
import io.servicefabric.transport.protocol.Message;
import io.servicefabric.transport.protocol.MessageDeserializer;
import io.servicefabric.transport.protocol.SchemaCache;

public final class ProtostuffMessageDeserializer implements MessageDeserializer {

	private final ITransportTypeRegistry typeRegistry;

	public ProtostuffMessageDeserializer(ITransportTypeRegistry typeRegistry) {
		this.typeRegistry = typeRegistry;
	}

	@Override
	public Message deserialize(ByteBuf bb) {
		Schema<BinaryMessage> schema = SchemaCache.getSchema(BinaryMessage.class);
		BinaryMessage binaryMessage = schema.newMessage();
		try {
			mergeFrom(new ByteBufInputStream(bb), binaryMessage, schema);
		} catch (Exception e) {
			throw new DecoderException(e.getMessage(), e);
		}

		Object data = deserializeData(binaryMessage);

		return new Message(binaryMessage.getQualifier(), data, binaryMessage.getCorrelationId());
	}

	public Object deserializeData(BinaryMessage message) {
		if (message.getData() == null) {
			return null;
		}
		Class<?> clazz = typeRegistry.resolveType(message.getQualifier());
		if (clazz != null) {
			Schema schema = SchemaCache.getSchema(clazz);
			Object data = schema.newMessage();
			mergeFrom(message.getData(), data, schema);
			return data;
		}
		return message.getData();
	}
}
