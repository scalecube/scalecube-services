package io.scalecube.services.auth;

public interface CredentialsSuppliers {

  CredentialsSupplier forServiceRole(String serviceRole);
}
