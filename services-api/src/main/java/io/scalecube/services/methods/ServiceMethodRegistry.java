package io.scalecube.services.methods;

import io.scalecube.services.ServiceInfo;
import java.util.List;

public interface ServiceMethodRegistry {

  void registerService(ServiceInfo serviceInfo);

  boolean containsInvoker(String qualifier);

  ServiceMethodInvoker getInvoker(String qualifier);

  List<ServiceMethodInvoker> listInvokers();

  List<ServiceInfo> listServices();
}
