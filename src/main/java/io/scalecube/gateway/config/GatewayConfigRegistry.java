package io.scalecube.gateway.config;

import io.scalecube.config.ConfigRegistry;
import io.scalecube.config.ConfigRegistrySettings;
import io.scalecube.config.audit.Slf4JConfigEventListener;
import io.scalecube.config.source.ClassPathConfigSource;
import io.scalecube.config.source.SystemEnvironmentConfigSource;
import io.scalecube.config.source.SystemPropertiesConfigSource;
import java.nio.file.Path;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class GatewayConfigRegistry {

  private static final String JMX_MBEAN_NAME = "io.scalecube.gateway.config:name=ConfigRegistry";
  private static final Pattern CONFIG_PATTERN = Pattern.compile("(.*)config(.*)?\\.properties");
  private static final Predicate<Path> PATH_PREDICATE =
      path -> CONFIG_PATTERN.matcher(path.toString()).matches();

  private final ConfigRegistry configRegistry;

  private GatewayConfigRegistry() {
    this.configRegistry =
        ConfigRegistry.create(
            ConfigRegistrySettings.builder()
                .addListener(new Slf4JConfigEventListener())
                .addLastSource("sys_prop", new SystemPropertiesConfigSource())
                .addLastSource("env_var", new SystemEnvironmentConfigSource())
                .addLastSource("cp", new ClassPathConfigSource(PATH_PREDICATE))
                .jmxMBeanName(JMX_MBEAN_NAME)
                .build());
  }

  public static ConfigRegistry configRegistry() {
    return new GatewayConfigRegistry().configRegistry;
  }
}
