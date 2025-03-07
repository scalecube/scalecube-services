package io.scalecube.services.auth;

import io.scalecube.services.methods.ServiceRoleDefinition;
import java.util.Collection;

public interface ServiceRolesProcessor {

  void process(Collection<ServiceRoleDefinition> serviceRoles);
}
