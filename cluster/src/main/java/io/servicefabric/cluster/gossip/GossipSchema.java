package io.servicefabric.cluster.gossip;

import static io.servicefabric.transport.utils.RecycleableLinkedBuffer.DEFAULT_MAX_CAPACITY;
import static io.protostuff.LinkedBuffer.MIN_BUFFER_SIZE;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.protostuff.runtime.RuntimeSchema;
import io.servicefabric.transport.utils.RecycleableLinkedBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import io.protostuff.*;

import javax.annotation.Nonnull;

final class GossipSchema implements Schema<Gossip> {
	private static final Logger LOGGER = LoggerFactory.getLogger(GossipSchema.class);

	private static final RecycleableLinkedBuffer recycleableLinkedBuffer =
			new RecycleableLinkedBuffer(MIN_BUFFER_SIZE, DEFAULT_MAX_CAPACITY);

	private static final Map<String, Integer> fieldMap = ImmutableMap.of(
			"gossipId", 1,
			"qualifier", 2,
			"data", 3,
			"dataClass", 4);

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
	public String getFieldName(int number) {
		switch (number) {
		case 1:
			return "gossipId";
		case 2:
			return "qualifier";
		case 3:
			return "data";
		case 4:
			return "dataClass";
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
		String dataClassName = null;
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
			case 4:
				dataClassName = input.readString();
				break;
			default:
				input.handleUnknownField(number, this);
			}
		}
		if (originalData != null) {
			if (dataClassName == null) {
				gossip.setData(originalData);
			} else {
				Optional<Class> optionalDataClass = classCache.getUnchecked(dataClassName);
				if (optionalDataClass.isPresent()) {
					Class<?> dataClass = optionalDataClass.get();
					Schema dataSchema = RuntimeSchema.getSchema(dataClass);
					Object data = dataSchema.newMessage();
					try {
						ProtostuffIOUtil.mergeFrom(originalData, data, dataSchema);
					} catch (Throwable e) {
						LOGGER.error("Failed to deserialize : {}", gossip);
						throw e;
					}
					gossip.setData(data);
				} else {
					gossip.setData(originalData);
				}
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
				Class<?> dataClass = gossip.getData().getClass();
				Schema dataSchema = RuntimeSchema.getSchema(dataClass);

				try (RecycleableLinkedBuffer rlb = recycleableLinkedBuffer.get()) {
					byte[] array = ProtostuffIOUtil.toByteArray(originalData, dataSchema, rlb.buffer());
					output.writeByteArray(3, array, false);
				}

				output.writeString(4, dataClass.getName(), false);
			}
		}
	}
}
