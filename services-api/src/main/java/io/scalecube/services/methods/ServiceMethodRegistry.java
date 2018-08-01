package io.scalecube.services.methods;

public interface ServiceMethodRegistry {

  void registerService(Object serviceInstance);

  boolean containsInvoker(String qualifier);

  ServiceMethodInvoker getInvoker(String qualifier);

}
