package io.scalecube.services.security.vault;

import com.bettercloud.vault.json.Json;
import com.bettercloud.vault.rest.Rest;
import com.bettercloud.vault.rest.RestException;
import com.bettercloud.vault.rest.RestResponse;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.auth.CredentialsSupplier;
import io.scalecube.services.security.ServiceTokens;
import io.scalecube.utils.MaskUtil;
import java.util.Collections;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;

public final class VaultServiceTokenCredentialsSupplier implements CredentialsSupplier {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(VaultServiceTokenCredentialsSupplier.class);

  private static final String VAULT_TOKEN_HEADER = "X-Vault-Token";

  private String serviceRole;
  private String vaultAddress;
  private Supplier<String> vaultTokenSupplier;
  private BiFunction<String, Map<String, String>, String> serviceTokenNameBuilder;

  public VaultServiceTokenCredentialsSupplier() {}

  private VaultServiceTokenCredentialsSupplier(VaultServiceTokenCredentialsSupplier other) {
    this.serviceRole = other.serviceRole;
    this.vaultAddress = other.vaultAddress;
    this.vaultTokenSupplier = other.vaultTokenSupplier;
    this.serviceTokenNameBuilder = other.serviceTokenNameBuilder;
  }

  /**
   * Setter for serviceRole.
   *
   * @param serviceRole serviceRole
   * @return new instance with applied setting
   */
  public VaultServiceTokenCredentialsSupplier serviceRole(String serviceRole) {
    final VaultServiceTokenCredentialsSupplier c = copy();
    c.serviceRole = serviceRole;
    return c;
  }

  /**
   * Setter for vaultAddress.
   *
   * @param vaultAddress vaultAddress
   * @return new instance with applied setting
   */
  public VaultServiceTokenCredentialsSupplier vaultAddress(String vaultAddress) {
    final VaultServiceTokenCredentialsSupplier c = copy();
    c.vaultAddress = vaultAddress;
    return c;
  }

  /**
   * Setter for vaultTokenSupplier.
   *
   * @param vaultTokenSupplier vaultTokenSupplier
   * @return new instance with applied setting
   */
  public VaultServiceTokenCredentialsSupplier vaultTokenSupplier(
      Supplier<String> vaultTokenSupplier) {
    final VaultServiceTokenCredentialsSupplier c = copy();
    c.vaultTokenSupplier = vaultTokenSupplier;
    return c;
  }

  /**
   * Setter for serviceTokenNameBuilder.
   *
   * @param serviceTokenNameBuilder serviceTokenNameBuilder
   * @return new instance with applied setting
   */
  public VaultServiceTokenCredentialsSupplier serviceTokenNameBuilder(
      BiFunction<String, Map<String, String>, String> serviceTokenNameBuilder) {
    final VaultServiceTokenCredentialsSupplier c = copy();
    c.serviceTokenNameBuilder = serviceTokenNameBuilder;
    return c;
  }

  @Override
  public Mono<Map<String, String>> apply(ServiceReference serviceReference) {
    return Mono.fromCallable(vaultTokenSupplier::get)
        .map(vaultToken -> rpcGetServiceToken(serviceReference.tags(), vaultToken))
        .doOnNext(response -> verifyOk(response.getStatus()))
        .map(this::toCredentials)
        .doOnSuccess(
            creds ->
                LOGGER.info(
                    "[rpcGetServiceToken] Successfully obtained vault service token: {}",
                    MaskUtil.mask(creds)));
  }

  private Map<String, String> toCredentials(RestResponse response) {
    return Collections.singletonMap(
        ServiceTokens.SERVICE_TOKEN_HEADER,
        Json.parse(new String(response.getBody()))
            .asObject()
            .get("data")
            .asObject()
            .get("token")
            .asString());
  }

  private RestResponse rpcGetServiceToken(Map<String, String> tags, String vaultToken) {
    String uri = buildVaultServiceTokenUri(tags);
    LOGGER.info("[rpcGetServiceToken] Getting vault service token (uri='{}')", uri);
    try {
      return new Rest().header(VAULT_TOKEN_HEADER, vaultToken).url(uri).get();
    } catch (RestException e) {
      LOGGER.error(
          "[rpcGetServiceToken] Failed to get vault service token (uri='{}'), cause: {}",
          uri,
          e.toString());
      throw Exceptions.propagate(e);
    }
  }

  private static void verifyOk(int status) {
    if (status != 200) {
      LOGGER.error("[rpcGetServiceToken] Not expected status ({}) returned", status);
      throw new IllegalStateException("Not expected status returned, status=" + status);
    }
  }

  private String buildVaultServiceTokenUri(Map<String, String> tags) {
    return new StringJoiner("/", vaultAddress, "")
        .add("v1/identity/oidc/token")
        .add(serviceTokenNameBuilder.apply(serviceRole, tags))
        .toString();
  }

  private VaultServiceTokenCredentialsSupplier copy() {
    return new VaultServiceTokenCredentialsSupplier(this);
  }
}
