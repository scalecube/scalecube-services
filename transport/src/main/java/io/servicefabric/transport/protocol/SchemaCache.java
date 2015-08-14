package io.servicefabric.transport.protocol;

import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * The Class SchemaCache.
 */
public class SchemaCache {

	/** The Constant schemaCacheMap. */
	private static final ConcurrentMap<Class<?>, Schema<?>> schemaCacheMap = new ConcurrentHashMap<>();

	private SchemaCache() {
		/** Do not create new instance */
	}

	/**
	 * Gets the schema.
	 *
	 * @param clazz the clazz
	 * @return the schema
	 */
	@SuppressWarnings("unchecked")
	public static <T> Schema<T> getSchema(final Class<T> clazz) {
		checkArgument(clazz != null, "Failed to resolve schema. Class can't be null.");

		if (!schemaCacheMap.containsKey(clazz)) {
			Schema<T> schema = createSchema(clazz);
			schemaCacheMap.putIfAbsent(clazz, schema);
		}
		return (Schema<T>) schemaCacheMap.get(clazz);
	}

	private static <T> Schema<T> createSchema(Class<T> clazz) {
		Schema<T> schema = RuntimeSchema.getSchema(clazz);
		RuntimeSchema.register(clazz, schema);
		return schema;
	}

	/**
	 * Register custom schema.
	 *
	 * @param <T> the generic type
	 * @param type the type
	 * @param schema the schema
	 */
	public static <T> void registerCustomSchema(final Class<T> type, final Schema<T> schema) {
		RuntimeSchema.register(type, schema);
		schemaCacheMap.put(type, schema);
	}

}
