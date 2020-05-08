package io.scalecube.services.methods;

import io.scalecube.services.ServiceInfo;
import io.scalecube.services.auth.AuthContextRegistry;
import java.util.List;

public interface ServiceMethodRegistry {

  void registerService(ServiceInfo serviceInfo, AuthContextRegistry authContextRegistry);

  boolean containsInvoker(String qualifier);

  ServiceMethodInvoker getInvoker(String qualifier);

  List<ServiceMethodInvoker> listInvokers();

  List<ServiceInfo> listServices();
}
