/**
 * 
 */
package io.servicefabric.cluster.gossip;

import static io.servicefabric.transport.utils.RecycleableLinkedBuffer.DEFAULT_MAX_CAPACITY;
import static io.protostuff.LinkedBuffer.MIN_BUFFER_SIZE;

import java.io.IOException;
import java.util.Map;

import io.servicefabric.transport.ITransportTypeRegistry;
import io.servicefabric.transport.protocol.SchemaCache;
import io.servicefabric.transport.utils.RecycleableLinkedBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import io.protostuff.*;

final class GossipSchema implements Schema<Gossip> {
	private static final Logger LOGGER = LoggerFactory.getLogger(GossipSchema.class);

	private static final RecycleableLinkedBuffer recycleableLinkedBuffer =
			new RecycleableLinkedBuffer(MIN_BUFFER_SIZE, DEFAULT_MAX_CAPACITY);

	private static final Map<String, Integer> fieldMap = ImmutableMap.of(
			"gossipId", 1,
			"qualifier", 2,
			"data", 3);

	private final ITransportTypeRegistry typeRegistry;

	GossipSchema(ITransportTypeRegistry typeRegistry) {
		this.typeRegistry = typeRegistry;
	}

	@Override
	public String getFieldName(int number) {
		switch (number) {
		case 1:
			return "gossipId";
		case 2:
			return "qualifier";
		case 3:
			return "data";
		default:
			return null;
		}
	}

	@Override
	public int getFieldNumber(String name) {
		return fieldMap.get(name);
	}

	@Override
	public boolean isInitialized(Gossip message) {
		return message != null && message.getQualifier() != null && message.getGossipId() != null
				&& message.getData() != null;
	}

	@Override
	public Gossip newMessage() {
		return new Gossip();
	}

	@Override
	public String messageName() {
		return Gossip.class.getSimpleName();
	}

	@Override
	public String messageFullName() {
		return Gossip.class.getName();
	}

	@Override
	public Class<? super Gossip> typeClass() {
		return Gossip.class;
	}

	@Override
	public void mergeFrom(Input input, Gossip gossip) throws IOException {
		boolean iterate = true;
		byte[] originalData = null;
		while (iterate) {
			int number = input.readFieldNumber(this);
			switch (number) {
			case 0:
				iterate = false;
				break;
			case 1:
				gossip.setGossipId(input.readString());
				break;
			case 2:
				gossip.setQualifier(input.readString());
				break;
			case 3:
				originalData = input.readByteArray();
				break;
			default:
				input.handleUnknownField(number, this);
			}
		}
		if (originalData != null) {
			Class<?> dataClazz = typeRegistry.resolveType(gossip.getQualifier());
			if (dataClazz == null) {
				gossip.setData(originalData);
			} else {
				Schema dataSchema = SchemaCache.getSchema(dataClazz);
				Object data = dataSchema.newMessage();
				try {
					ProtostuffIOUtil.mergeFrom(originalData, data, dataSchema);
				} catch (Throwable e) {
					LOGGER.error("Failed to deserialize : {}", gossip);
					throw e;
				}
				gossip.setData(data);
			}
		}
	}

	@Override
	public void writeTo(Output output, Gossip gossip) throws IOException {
		if (gossip.getGossipId() != null) {
			output.writeString(1, gossip.getGossipId(), false);
		}
		if (gossip.getQualifier() != null) {
			output.writeString(2, gossip.getQualifier(), false);
		}
		Object originalData = gossip.getData();
		if (originalData != null) {
			if (originalData instanceof byte[]) {
				output.writeByteArray(3, (byte[]) originalData, false);
			} else {
				Class<?> dataClazz = typeRegistry.resolveType(gossip.getQualifier());
				if (dataClazz == null) {
					throw new RuntimeException("Can't serialize data for qualifier " + gossip.getQualifier());
				}
				Schema dataSchema = SchemaCache.getSchema(dataClazz);

				try (RecycleableLinkedBuffer rlb = recycleableLinkedBuffer.get()) {
					byte[] array = ProtostuffIOUtil.toByteArray(originalData, dataSchema, rlb.buffer());
					output.writeByteArray(3, array, false);
				}
			}
		}
	}
}
