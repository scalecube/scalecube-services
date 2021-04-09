package io.scalecube.services.security.vault;

import com.bettercloud.vault.json.Json;
import com.bettercloud.vault.rest.Rest;
import com.bettercloud.vault.rest.RestException;
import com.bettercloud.vault.rest.RestResponse;
import io.scalecube.utils.MaskUtil;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;

public final class VaultServiceTokenSupplier {

  private static final Logger LOGGER = LoggerFactory.getLogger(VaultServiceTokenSupplier.class);

  private static final String VAULT_TOKEN_HEADER = "X-Vault-Token";

  private String serviceRole;
  private String vaultAddress;
  private Supplier<String> vaultTokenSupplier;
  private BiFunction<String, Map<String, String>, String> serviceTokenNameBuilder;

  public VaultServiceTokenSupplier() {}

  private VaultServiceTokenSupplier(VaultServiceTokenSupplier other) {
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
  public VaultServiceTokenSupplier serviceRole(String serviceRole) {
    final VaultServiceTokenSupplier c = copy();
    c.serviceRole = serviceRole;
    return c;
  }

  /**
   * Setter for vaultAddress.
   *
   * @param vaultAddress vaultAddress
   * @return new instance with applied setting
   */
  public VaultServiceTokenSupplier vaultAddress(String vaultAddress) {
    final VaultServiceTokenSupplier c = copy();
    c.vaultAddress = vaultAddress;
    return c;
  }

  /**
   * Setter for vaultTokenSupplier.
   *
   * @param vaultTokenSupplier vaultTokenSupplier
   * @return new instance with applied setting
   */
  public VaultServiceTokenSupplier vaultTokenSupplier(Supplier<String> vaultTokenSupplier) {
    final VaultServiceTokenSupplier c = copy();
    c.vaultTokenSupplier = vaultTokenSupplier;
    return c;
  }

  /**
   * Setter for serviceTokenNameBuilder.
   *
   * @param serviceTokenNameBuilder serviceTokenNameBuilder; inputs for this function are {@code
   *     serviceRole} and {@code tags} attributes
   * @return new instance with applied setting
   */
  public VaultServiceTokenSupplier serviceTokenNameBuilder(
      BiFunction<String, Map<String, String>, String> serviceTokenNameBuilder) {
    final VaultServiceTokenSupplier c = copy();
    c.serviceTokenNameBuilder = serviceTokenNameBuilder;
    return c;
  }

  /**
   * Returns credentials as {@code Map<String, String>} for the given args.
   *
   * @param tags tags attributes
   * @return vault service token
   */
  public Mono<String> getServiceToken(Map<String, String> tags) {
    return Mono.fromCallable(vaultTokenSupplier::get)
        .map(vaultToken -> rpcGetServiceToken(tags, vaultToken))
        .doOnNext(response -> verifyOk(response.getStatus()))
        .map(
            response ->
                Json.parse(new String(response.getBody()))
                    .asObject()
                    .get("data")
                    .asObject()
                    .get("token")
                    .asString())
        .doOnSuccess(
            creds ->
                LOGGER.info(
                    "[rpcGetServiceToken] Successfully obtained vault service token: {}",
                    MaskUtil.mask(creds)));
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

  private VaultServiceTokenSupplier copy() {
    return new VaultServiceTokenSupplier(this);
  }
}
