package io.scalecube.services.security;

import io.scalecube.security.vault.VaultServiceTokenSupplier;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.transport.api.ClientTransport;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;

public class ServiceTokenCredentialsSupplier implements ClientTransport.CredentialsSupplier {

  private final String environment;
  private final String vaultAddress;
  private final Supplier<CompletableFuture<String>> vaultTokenSupplier;

  public ServiceTokenCredentialsSupplier(
      String environment,
      String vaultAddress,
      Supplier<CompletableFuture<String>> vaultTokenSupplier) {
    this.environment = environment;
    this.vaultAddress = vaultAddress;
    this.vaultTokenSupplier = vaultTokenSupplier;
  }

  @Override
  public Mono<byte[]> credentials(ServiceReference serviceReference, String serviceRole) {
    return Mono.defer(
        () -> {
          if (serviceRole == null) {
            return Mono.just(new byte[0]);
          }

          return Mono.fromFuture(
                  VaultServiceTokenSupplier.builder()
                      .vaultAddress(vaultAddress)
                      .serviceRole(serviceRole)
                      .vaultTokenSupplier(vaultTokenSupplier)
                      .serviceTokenNameBuilder((role, tags) -> environment + "." + role)
                      .build()
                      .getToken(serviceReference.tags()))
              .map(String::getBytes);
        });
  }
}
