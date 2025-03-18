package io.scalecube.services.security;

import io.scalecube.security.vault.VaultServiceTokenSupplier;
import io.scalecube.services.auth.CredentialsSupplier;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;

public class ServiceTokenCredentialsSupplier implements CredentialsSupplier {

  private final String environment;
  private final String vaultAddress;
  private final Supplier<CompletableFuture<String>> vaultTokenSupplier;

  public ServiceTokenCredentialsSupplier(
      String environment,
      String vaultAddress,
      Supplier<CompletableFuture<String>> vaultTokenSupplier) {
    this.environment = Objects.requireNonNull(environment, "environment");
    this.vaultAddress = Objects.requireNonNull(vaultAddress, "vaultAddress");
    this.vaultTokenSupplier = Objects.requireNonNull(vaultTokenSupplier, "vaultTokenSupplier");
  }

  @Override
  public Mono<byte[]> credentials(String serviceRole) {
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
                      .getToken(Collections.emptyMap()))
              .map(String::getBytes);
        });
  }
}
