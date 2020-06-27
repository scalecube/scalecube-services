package io.scalecube.services.methods;

import io.scalecube.services.ServiceInfo;
import java.util.List;

public interface ServiceMethodRegistry {

  void registerService(ServiceInfo serviceInfo);

  ServiceMethodInvoker getInvoker(String qualifier);

  List<ServiceMethodInvoker> listInvokers();

  List<ServiceInfo> listServices();
}
