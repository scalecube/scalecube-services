package io.scalecube.services;

import java.util.Properties;
import java.util.function.Consumer;

/**
 * Adds or overrides entries of the {@link Properties} a component is configured from, for example
 * the properties passed to a transport. Lets each part of a setup contribute its own properties.
 */
@FunctionalInterface
public interface PropertiesConfigurer extends Consumer<Properties> {}
