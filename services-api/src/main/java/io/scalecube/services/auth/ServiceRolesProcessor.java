package io.scalecube.services.auth;

import io.scalecube.services.methods.ServiceRoleDefinition;
import java.util.Collection;

/**
 * Handler for processing of service roles which come out of registered services. Used as
 * post-construction step in bootstraping of services.
 */
public interface ServiceRolesProcessor {

  /**
   * Function that does processing of service roles.
   *
   * @param values collection {@link ServiceRoleDefinition} objects
   */
  void process(Collection<ServiceRoleDefinition> values);
}
