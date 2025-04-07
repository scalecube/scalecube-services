package io.scalecube.services.security.environment;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

public class IntegrationEnvironmentFixture
    implements BeforeAllCallback, ExtensionContext.Store.CloseableResource, ParameterResolver {

  private static final Map<Class<?>, Supplier<?>> PARAMETERS_TO_RESOLVE = new HashMap<>();

  private static VaultEnvironment vaultEnvironment;

  @Override
  public void beforeAll(ExtensionContext context) {
    context
        .getRoot()
        .getStore(Namespace.GLOBAL)
        .getOrComputeIfAbsent(
            this.getClass(),
            key -> {
              vaultEnvironment = VaultEnvironment.start();
              return this;
            });

    PARAMETERS_TO_RESOLVE.put(VaultEnvironment.class, () -> vaultEnvironment);
  }

  @Override
  public void close() {
    if (vaultEnvironment != null) {
      vaultEnvironment.close();
    }
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Class<?> type = parameterContext.getParameter().getType();
    return PARAMETERS_TO_RESOLVE.keySet().stream().anyMatch(type::isAssignableFrom);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Class<?> type = parameterContext.getParameter().getType();
    return PARAMETERS_TO_RESOLVE.get(type).get();
  }
}
