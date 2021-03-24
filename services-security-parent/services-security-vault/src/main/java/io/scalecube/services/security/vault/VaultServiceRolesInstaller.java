package io.scalecube.services.security.vault;

import com.bettercloud.vault.json.Json;
import com.bettercloud.vault.rest.Rest;
import com.bettercloud.vault.rest.RestException;
import io.scalecube.services.security.vault.VaultServiceRolesInstaller.ServiceRoles.Role;
import java.io.InputStream;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import reactor.core.Exceptions;

public final class VaultServiceRolesInstaller {

  private static final Logger LOGGER = LoggerFactory.getLogger(VaultServiceRolesInstaller.class);

  private static final String VAULT_TOKEN_HEADER = "X-Vault-Token";

  private String vaultAddress;
  private Supplier<String> vaultTokenSupplier;
  private Supplier<String> keyNameSupplier;
  private Function<String, String> roleNameBuilder;
  private String inputFileName = "service-roles.yaml";
  private String keyAlgorithm = "RS256";
  private String keyRotationPeriod = "1h";
  private String keyVerificationTtl = "1h";
  private String roleTtl = "1m";

  public VaultServiceRolesInstaller() {}

  private VaultServiceRolesInstaller(VaultServiceRolesInstaller other) {
    this.vaultAddress = other.vaultAddress;
    this.vaultTokenSupplier = other.vaultTokenSupplier;
    this.keyNameSupplier = other.keyNameSupplier;
    this.roleNameBuilder = other.roleNameBuilder;
    this.inputFileName = other.inputFileName;
    this.keyAlgorithm = other.keyAlgorithm;
    this.keyRotationPeriod = other.keyRotationPeriod;
    this.keyVerificationTtl = other.keyVerificationTtl;
    this.roleTtl = other.roleTtl;
  }

  /**
   * Setter for vaultAddress.
   *
   * @param vaultAddress vaultAddress
   * @return new instance with applied setting
   */
  public VaultServiceRolesInstaller vaultAddress(String vaultAddress) {
    final VaultServiceRolesInstaller c = copy();
    c.vaultAddress = vaultAddress;
    return c;
  }

  /**
   * Setter for vaultTokenSupplier.
   *
   * @param vaultTokenSupplier vaultTokenSupplier
   * @return new instance with applied setting
   */
  public VaultServiceRolesInstaller vaultTokenSupplier(Supplier<String> vaultTokenSupplier) {
    final VaultServiceRolesInstaller c = copy();
    c.vaultTokenSupplier = vaultTokenSupplier;
    return c;
  }

  /**
   * Setter for keyNameSupplier.
   *
   * @param keyNameSupplier keyNameSupplier
   * @return new instance with applied setting
   */
  public VaultServiceRolesInstaller keyNameSupplier(Supplier<String> keyNameSupplier) {
    final VaultServiceRolesInstaller c = copy();
    c.keyNameSupplier = keyNameSupplier;
    return c;
  }

  /**
   * Setter for roleNameBuilder.
   *
   * @param roleNameBuilder roleNameBuilder
   * @return new instance with applied setting
   */
  public VaultServiceRolesInstaller roleNameBuilder(Function<String, String> roleNameBuilder) {
    final VaultServiceRolesInstaller c = copy();
    c.roleNameBuilder = roleNameBuilder;
    return c;
  }

  /**
   * Setter for inputFileName.
   *
   * @param inputFileName inputFileName
   * @return new instance with applied setting
   */
  public VaultServiceRolesInstaller inputFileName(String inputFileName) {
    final VaultServiceRolesInstaller c = copy();
    c.inputFileName = inputFileName;
    return c;
  }

  /**
   * Setter for keyAlgorithm.
   *
   * @param keyAlgorithm keyAlgorithm
   * @return new instance with applied setting
   */
  public VaultServiceRolesInstaller keyAlgorithm(String keyAlgorithm) {
    final VaultServiceRolesInstaller c = copy();
    c.keyAlgorithm = keyAlgorithm;
    return c;
  }

  /**
   * Setter for keyRotationPeriod.
   *
   * @param keyRotationPeriod keyRotationPeriod
   * @return new instance with applied setting
   */
  public VaultServiceRolesInstaller keyRotationPeriod(String keyRotationPeriod) {
    final VaultServiceRolesInstaller c = copy();
    c.keyRotationPeriod = keyRotationPeriod;
    return c;
  }

  /**
   * Setter for keyVerificationTtl.
   *
   * @param keyVerificationTtl keyVerificationTtl
   * @return new instance with applied setting
   */
  public VaultServiceRolesInstaller keyVerificationTtl(String keyVerificationTtl) {
    final VaultServiceRolesInstaller c = copy();
    c.keyVerificationTtl = keyVerificationTtl;
    return c;
  }

  /**
   * Setter for roleTtl.
   *
   * @param roleTtl roleTtl
   * @return new instance with applied setting
   */
  public VaultServiceRolesInstaller roleTtl(String roleTtl) {
    final VaultServiceRolesInstaller c = copy();
    c.roleTtl = roleTtl;
    return c;
  }

  /**
   * Reads {@code serviceRolesFileName (access-file.yaml)} and builds micro-infrastructure for
   * machine-to-machine authentication in the vault.
   */
  public void install() {
    if (isNullOrNoneOrEmpty(vaultAddress)) {
      return;
    }

    final ServiceRoles serviceRoles = loadServiceRoles();
    if (serviceRoles == null) {
      return;
    }

    final Rest rest = new Rest().header(VAULT_TOKEN_HEADER, vaultTokenSupplier.get());

    if (!serviceRoles.roles.isEmpty()) {
      String keyName = keyNameSupplier.get();
      createVaultIdentityKey(keyName, () -> rest.url(buildVaultIdentityKeyUri(keyName)));

      for (Role role : serviceRoles.roles) {
        String roleName = roleNameBuilder.apply(role.role);
        createVaultIdentityRole(
            keyName,
            roleName,
            role.permissions,
            () -> rest.url(buildVaultIdentityRoleUri(roleName)));
      }
    }
  }

  private ServiceRoles loadServiceRoles() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    InputStream inputStream = classLoader.getResourceAsStream(inputFileName);
    return inputStream != null
        ? new Yaml(new Constructor(ServiceRoles.class)).load(inputStream)
        : null;
  }

  private static void verifyOk(int status, String operation) {
    if (status != 200 && status != 204) {
      LOGGER.error("Not expected status ({}) returned on [{}]", status, operation);
      throw new IllegalStateException("Not expected status returned, status=" + status);
    }
  }

  private void createVaultIdentityKey(String keyName, Supplier<Rest> restSupplier) {
    LOGGER.debug("[createVaultIdentityKey] {}", keyName);

    byte[] body =
        Json.object()
            .add("rotation_period", keyRotationPeriod)
            .add("verification_ttl", keyVerificationTtl)
            .add("allowed_client_ids", "*")
            .add("algorithm", keyAlgorithm)
            .toString()
            .getBytes();

    try {
      verifyOk(restSupplier.get().body(body).post().getStatus(), "createVaultIdentityKey");
    } catch (RestException e) {
      throw Exceptions.propagate(e);
    }
  }

  private void createVaultIdentityRole(
      String keyName, String roleName, List<String> permissions, Supplier<Rest> restSupplier) {
    LOGGER.debug("[createVaultIdentityRole] {}", roleName);

    byte[] body =
        Json.object()
            .add("key", keyName)
            .add("template", createTemplate(permissions))
            .add("ttl", roleTtl)
            .toString()
            .getBytes();

    try {
      verifyOk(restSupplier.get().body(body).post().getStatus(), "createVaultIdentityRole");
    } catch (RestException e) {
      throw Exceptions.propagate(e);
    }
  }

  private static String createTemplate(List<String> permissions) {
    return Base64.getUrlEncoder()
        .encodeToString(
            Json.object().add("permissions", String.join(",", permissions)).toString().getBytes());
  }

  private String buildVaultIdentityKeyUri(String keyName) {
    return new StringJoiner("/", vaultAddress, "")
        .add("v1/identity/oidc/key")
        .add(keyName)
        .toString();
  }

  private String buildVaultIdentityRoleUri(String roleName) {
    return new StringJoiner("/", vaultAddress, "")
        .add("v1/identity/oidc/role")
        .add(roleName)
        .toString();
  }

  private VaultServiceRolesInstaller copy() {
    return new VaultServiceRolesInstaller(this);
  }

  private static boolean isNullOrNoneOrEmpty(String value) {
    return Objects.isNull(value)
        || "none".equalsIgnoreCase(value)
        || "null".equals(value)
        || value.isEmpty();
  }

  public static class ServiceRoles {

    private List<Role> roles;

    public List<Role> getRoles() {
      return roles;
    }

    public void setRoles(List<Role> roles) {
      this.roles = roles;
    }

    public static class Role {

      private String role;
      private List<String> permissions;

      public String getRole() {
        return role;
      }

      public void setRole(String role) {
        this.role = role;
      }

      public List<String> getPermissions() {
        return permissions;
      }

      public void setPermissions(List<String> permissions) {
        this.permissions = permissions;
      }
    }
  }
}
