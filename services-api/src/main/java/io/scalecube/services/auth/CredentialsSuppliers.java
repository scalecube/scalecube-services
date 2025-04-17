package io.scalecube.services.auth;

import java.util.List;

public class CredentialsSuppliers {

  private final CredentialsSupplier credentialsSupplier;

  public CredentialsSuppliers(CredentialsSupplier credentialsSupplier) {
    this.credentialsSupplier = credentialsSupplier;
  }

  public CredentialsSupplier forServiceRole(String serviceRole) {
    return (service, allowedRoles) ->
        credentialsSupplier.credentials(service, List.of(serviceRole));
  }
}
