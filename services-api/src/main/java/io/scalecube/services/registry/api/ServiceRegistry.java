package io.scalecube.services.registry.api;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.methods.ServiceMethodInvoker;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import reactor.core.scheduler.Scheduler;

/**
 * Service registry interface provides API to register/unregister services in the system and make
 * services lookup by service result.
 */
public interface ServiceRegistry {

  List<ServiceEndpoint> listServiceEndpoints();

  List<ServiceReference> listServiceReferences();

  List<ServiceReference> lookupService(ServiceMessage request);

  boolean registerService(ServiceEndpoint serviceEndpoint);

  void registerService(ServiceInfo serviceInfo);

  void registerService(
      ServiceInfo serviceInfo,
      Map<String, Scheduler> schedulers,
      UnaryOperator<String> qualifierOperator);

  ServiceEndpoint unregisterService(String endpointId);

  List<ServiceInfo> listServices();

  ServiceMethodInvoker getInvoker(ServiceMessage request);
}
