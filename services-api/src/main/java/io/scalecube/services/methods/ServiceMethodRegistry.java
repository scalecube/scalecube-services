package io.scalecube.services.methods;

public interface ServiceMethodRegistry {

  boolean containsInvoker(String qualifier);

  ServiceMethodInvoker getInvoker(String qualifier);
}
