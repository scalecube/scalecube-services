package io.scalecube.services;

import java.util.Properties;

/**
 * Configures {@link Properties} before a component (for example a transport) is created from them.
 * Lets the properties be contributed by independent parts of a setup, each applied to the same
 * {@code Properties}, for example one per bean of a dependency-injection container.
 */
@FunctionalInterface
public interface PropertiesConfigurer {

  /**
   * Applies this configuration to the properties.
   *
   * @param properties properties to configure
   */
  void configure(Properties properties);
}
