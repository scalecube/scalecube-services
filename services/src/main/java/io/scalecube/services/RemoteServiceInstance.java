package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.transport.Address;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class RemoteServiceInstance implements ServiceInstance {

  private final Address address;
  private final String memberId;
  private final String serviceName;
  private final Map<String, String> tags;
  private final Set<String> methods;

  /**
   * Remote service instance constructor to initiate instance.
   * 
   * @param address
   * @param memberId
   * @param serviceName
   * @param methods
   * @param tags describing this service instance metadata.
   */
  public RemoteServiceInstance(Address address,
      String memberId,
      String serviceName,
      Set<String> methods,
      Map<String, String> tags) {
    this.serviceName = serviceName;
    this.methods = Collections.unmodifiableSet(methods);
    this.address = address;
    this.memberId = memberId;
    this.tags = tags;
  }

  @Override
  public String memberId() {
    return this.memberId;
  }

  public Address address() {
    return address;
  }

  @Override
  public Boolean isLocal() {
    return false;
  }

  @Override
  public Map<String, String> tags() {
    return tags;
  }

  @Override
  public String serviceName() {
    return serviceName;
  }

  @Override
  public boolean methodExists(String methodName) {
    return methods.contains(methodName);
  }

  @Override
  public void checkMethodExists(String methodName) {
    checkArgument(this.methodExists(methodName), "instance has no such requested method");
  }

  @Override
  public Collection<String> methods() {
    return methods;
  }

  @Override
  public String toString() {
    return "RemoteServiceInstance [serviceName=" + serviceName + ", address=" + address + ", memberId=" + memberId
        + ", methods=" + methods + ", tags=" + tags + "]";
  }


}
