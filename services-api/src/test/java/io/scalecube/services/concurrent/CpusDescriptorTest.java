package io.scalecube.services.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CpusDescriptorTest {

  @TempDir Path tempDir;

  @Test
  void shouldReturnEffectiveCpusWhenSharedCountIsProvided() throws IOException {
    // Create fake cpuset "0-4" (5 CPUs)
    final var path = tempDir.resolve("cpuset.cpus.effective");
    Files.writeString(path, "0-4");

    final var properties = new Properties();
    properties.setProperty("SHARED_CPU_COUNT", "2");

    final var descriptor = CpusDescriptor.from(properties, path);

    assertEquals(2, descriptor.sharedCpuCount());
    assertIterableEquals(List.of(0, 1, 2, 3, 4), descriptor.cpusetCpus());

    // Should skip first 2: [0, 1] skipped -> [2, 3, 4] remains
    assertIterableEquals(List.of(2, 3, 4), descriptor.effectiveCpusetCpus());
  }

  @Test
  void shouldHandleComplexCpuSpecs() throws IOException {
    final var path = tempDir.resolve("cpuset_complex");
    Files.writeString(path, "0,2-3,6");

    final var properties = new Properties();
    properties.setProperty("SHARED_CPU_COUNT", "1");

    final var descriptor = CpusDescriptor.from(properties, path);

    assertIterableEquals(List.of(0, 2, 3, 6), descriptor.cpusetCpus());

    assertIterableEquals(List.of(2, 3, 6), descriptor.effectiveCpusetCpus());
  }

  @Test
  void shouldReturnNullEffectiveCpusWhenSharedCountExceedsAvailableCpus() throws IOException {
    final var path = tempDir.resolve("cpuset_small");
    Files.writeString(path, "0-1"); // 2 CPUs

    final var properties = new Properties();
    properties.setProperty("SHARED_CPU_COUNT", "5"); // More than available

    final var descriptor = CpusDescriptor.from(properties, path);

    assertNull(descriptor.effectiveCpusetCpus());
  }

  @Test
  void shouldDefaultToZeroWhenPropertyIsMissing() throws IOException {
    final var path = tempDir.resolve("cpuset_default");
    Files.writeString(path, "0-2");

    final var descriptor = CpusDescriptor.from(new Properties(), path);

    assertEquals(0, descriptor.sharedCpuCount());
    assertNull(descriptor.effectiveCpusetCpus());
  }
}
