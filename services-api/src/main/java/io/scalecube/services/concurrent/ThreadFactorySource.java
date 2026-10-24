package io.scalecube.services.concurrent;

import java.util.concurrent.ThreadFactory;

/**
 * A source for creating {@link ThreadFactory} instances based on configuration string. Allows
 * dynamic creation of {@link ThreadFactory} that may implement specific behavior, such as CPU
 * affinity.
 */
public interface ThreadFactorySource {

  /**
   * Creates {@link ThreadFactory} according to the provided specification.
   *
   * @param spec configuration string (e.g., "cpu:0"), may be null
   * @return {@link ThreadFactory} instance, or {@code null} if the spec is unsupported
   */
  ThreadFactory threadFactory(String spec);
}
