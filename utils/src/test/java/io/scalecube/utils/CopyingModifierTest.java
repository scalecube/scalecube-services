package io.scalecube.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class CopyingModifierTest {

  @Test
  public void testCopyingModifierConfig() throws Exception {
    ConfigurableComponent component = new ConfigurableComponent(new Config()).setA(1).setB(2);
    assertEquals(1, component.config.a);
    assertEquals(2, component.config.b);
    assertEquals(300, component.config.c);
  }

  private static class Config implements CopyingModifier<Config> {
    private int a = 100, b = 200, c = 300;

    private Config() {}

    private Config(Config other) {
      this.a = other.a;
      this.b = other.b;
      this.c = other.c;
    }
  }

  private static class ConfigurableComponent {
    private final Config config;

    private ConfigurableComponent(Config config) {
      this.config = config;
    }

    private ConfigurableComponent setA(int a) {
      return new ConfigurableComponent(config.copyAndSet(cfg -> cfg.a = a));
    }

    private ConfigurableComponent setB(int b) {
      return new ConfigurableComponent(config.copyAndSet(cfg -> cfg.b = b));
    }

    private ConfigurableComponent setC(int c) {
      return new ConfigurableComponent(config.copyAndSet(cfg -> cfg.c = c));
    }
  }
}
