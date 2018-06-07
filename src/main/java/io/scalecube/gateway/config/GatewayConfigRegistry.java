package io.scalecube.gateway.config;

import io.scalecube.config.ConfigRegistry;
import io.scalecube.config.ConfigRegistrySettings;
import io.scalecube.config.audit.Slf4JConfigEventListener;
import io.scalecube.config.source.ClassPathConfigSource;
import io.scalecube.config.source.SystemEnvironmentConfigSource;
import io.scalecube.config.source.SystemPropertiesConfigSource;

import java.util.regex.Pattern;

public class GatewayConfigRegistry {

  public static final String JMX_MBEAN_NAME = "io.scalecube.gateway.config:name=ConfigRegistry";
  public static final Pattern CONFIG_FILENAME_PATTERN = Pattern.compile(".*/config/(.*)config(.*)?\\.properties");
  public static final int RELOAD_INTERVAL_SEC = 300;

  private final ConfigRegistry configRegistry;

  private GatewayConfigRegistry() {
    this.configRegistry = ConfigRegistry.create(ConfigRegistrySettings.builder()
        .addListener(new Slf4JConfigEventListener())
        .addLastSource("sys_prop", new SystemPropertiesConfigSource())
        .addLastSource("env_var", new SystemEnvironmentConfigSource())
        .addLastSource("cp",
            new ClassPathConfigSource(path -> CONFIG_FILENAME_PATTERN.matcher(path.toString()).matches()))
        .jmxMBeanName(JMX_MBEAN_NAME)
        .reloadIntervalSec(RELOAD_INTERVAL_SEC)
        .build());
  }

  public static ConfigRegistry configRegistry() {
    return new GatewayConfigRegistry().configRegistry;
  }
}
