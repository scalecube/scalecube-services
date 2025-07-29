package io.scalecube.services.auth;

/**
 * Factory for {@link CredentialsSupplier} objects. Responsible for producing {@link
 * CredentialsSupplier} for service role that is known upfront.
 */
public class CredentialsSuppliers {

  private final CredentialsSupplier credentialsSupplier;

  public CredentialsSuppliers(CredentialsSupplier credentialsSupplier) {
    this.credentialsSupplier = credentialsSupplier;
  }

  /**
   * Returns {@link CredentialsSupplier} instance per given {@code serviceRole}.
   *
   * @param serviceRole serviceRole
   * @return {@link CredentialsSupplier} instance
   */
  public CredentialsSupplier forServiceRole(String serviceRole) {
    return (service, allowedRoles) -> credentialsSupplier.credentials(service, serviceRole);
  }
}
