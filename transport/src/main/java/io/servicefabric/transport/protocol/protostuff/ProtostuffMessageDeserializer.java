package io.servicefabric.transport.protocol.protostuff;

import static io.protostuff.ProtostuffIOUtil.mergeFrom;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.DecoderException;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import io.servicefabric.transport.protocol.Message;
import io.servicefabric.transport.protocol.MessageDeserializer;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

public final class ProtostuffMessageDeserializer implements MessageDeserializer {

	private final LoadingCache<String, Optional<Class>> classCache = CacheBuilder.newBuilder()
			.expireAfterAccess(1, TimeUnit.HOURS)
			.build(new CacheLoader<String, Optional<Class>>() {
				@Override
				public Optional<Class> load(@Nonnull String className) throws Exception {
					try {
						Class dataClass = Class.forName(className);
						return Optional.of(dataClass);
					} catch (ClassNotFoundException e) {
						return Optional.absent();
					}
				}
			});

	@Override
	public Message deserialize(ByteBuf bb) {
		// Deserialize BinaryMessage
		Schema<BinaryMessage> schema = RuntimeSchema.getSchema(BinaryMessage.class);
		BinaryMessage binaryMessage = schema.newMessage();
		try {
			mergeFrom(new ByteBufInputStream(bb), binaryMessage, schema);
		} catch (Exception e) {
			throw new DecoderException(e.getMessage(), e);
		}

		// Deserialize data
		Object data = deserializeData(binaryMessage);

		// Convert BinaryMessage to Message
		return new Message(binaryMessage.getQualifier(), data, binaryMessage.getCorrelationId());
	}

	public Object deserializeData(BinaryMessage message) {
		// Handle null data
		if (message.getData() == null) {
			return null;
		}

		// Handle binary data
		if (message.getDataClass() == null) {
			return message.getData();
		}

		// Handle object data
		Optional<Class> optionalDataClass = classCache.getUnchecked(message.getDataClass());
		if (optionalDataClass.isPresent()) {
			Class<?> dataClass = optionalDataClass.get();
			Schema schema = RuntimeSchema.getSchema(dataClass);
			Object data = schema.newMessage();
			mergeFrom(message.getData(), data, schema);
			return data;
		} else {
			// Handle unknown object data (do not deserialize payload)
			return message.getData();
		}
	}
}
