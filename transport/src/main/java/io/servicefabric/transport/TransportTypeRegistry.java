package io.servicefabric.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TransportTypeRegistry implements ITransportTypeRegistry {

	private static final Logger LOGGER = LoggerFactory.getLogger(TransportTypeRegistry.class);

	private static final TransportTypeRegistry instance = new TransportTypeRegistry();

	private final ConcurrentMap<String, Class<?>> types = new ConcurrentHashMap<>();

	private TransportTypeRegistry() {
		/* Do not instantiate */
	}

	public static ITransportTypeRegistry getInstance() {
		return instance;
	}

	@Override
	public Class<?> resolveType(String qualifier) {
		if (qualifier == null) {
			return null;
		}
		return types.get(qualifier.toLowerCase());
	}

	@Override
	public void registerType(final String qualifier, final Class<?> clazz) {
		Class<?> prevClazz = types.putIfAbsent(qualifier.toLowerCase(), clazz);
		if (prevClazz == null) {
			LOGGER.debug("Registered type with qualifier {} for the class {}", qualifier, clazz);
		} else if (!prevClazz.equals(clazz)) {
			LOGGER.warn("Can't replace type {} with {} for already registered qualifier {}", prevClazz,	clazz, qualifier);
		}
	}

}
