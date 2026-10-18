package io.scalecube.services;

import java.util.Properties;

/**
 * Sets entries on a {@link Properties} object. Several configurers can be applied, one after
 * another, to the same {@code Properties} before it is used; a later configurer overrides an entry
 * set by an earlier one.
 */
@FunctionalInterface
public interface PropertiesConfigurer {

  /**
   * Sets this configurer's entries on the given properties.
   *
   * @param properties properties to configure
   */
  void configure(Properties properties);
}
