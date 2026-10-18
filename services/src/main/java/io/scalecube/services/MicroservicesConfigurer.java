package io.scalecube.services;

/**
 * Configures a {@link Microservices.Context} before {@link Microservices#start(Microservices.Context)
 * start}. Lets a setup be split into independent parts (transport, discovery, gateways, services,
 * ...), each applied to the same context, for example one per bean of a dependency-injection
 * container.
 */
@FunctionalInterface
public interface MicroservicesConfigurer {

  /**
   * Applies this configuration to the context.
   *
   * @param context context to configure
   */
  void configure(Microservices.Context context);
}
