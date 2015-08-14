package io.servicefabric.transport;

/**
 * The Interface ITypeResolver.
 */
public interface ITransportTypeRegistry {

	/**
	 * Resolve type class by the given qualifier.
	 *
	 * @param qualifier the qualifier
	 * @return the class
	 */
	Class<?> resolveType(String qualifier);

	/**
	 * Register type with the given qualifier and class.
	 *
	 * @param qualifier the qualifier
	 * @param clazz the clazz
	 */
	void registerType(final String qualifier, final Class<?> clazz);

}
