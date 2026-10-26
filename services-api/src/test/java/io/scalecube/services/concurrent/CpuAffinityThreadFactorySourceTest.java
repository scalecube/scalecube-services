package io.scalecube.services.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.BitSet;
import java.util.List;
import org.junit.jupiter.api.Test;

class CpuAffinityThreadFactorySourceTest {

  // CPUs 0-7 allocated to the cgroup, SHARED_CPU_COUNT=4 -> effective (pinnable) CPUs are 4-7.
  private static final CpusDescriptor AFFINITY_ENABLED =
      new CpusDescriptor(4, List.of(0, 1, 2, 3, 4, 5, 6, 7), List.of(4, 5, 6, 7));

  private static final CpusDescriptor AFFINITY_DISABLED =
      new CpusDescriptor(0, List.of(0, 1, 2, 3, 4, 5, 6, 7), null);

  @Test
  void shouldReturnNullThreadFactoryWhenAffinityIsDisabled() {
    final var source = new CpuAffinityThreadFactorySource(AFFINITY_DISABLED);

    assertNull(source.threadFactory("cpu:0"));
    assertNull(source.threadFactory("cpu:-1"));
    assertNull(source.threadFactory(null));
  }

  @Test
  void shouldReturnNullThreadFactoryWhenEffectiveCpusIsEmpty() {
    final var source =
        new CpuAffinityThreadFactorySource(new CpusDescriptor(4, List.of(0, 1, 2, 3), List.of()));

    assertNull(source.threadFactory("cpu:0"));
  }

  @Test
  void shouldReturnNullCpuMaskWhenSpecIsNull() {
    final var source = new CpuAffinityThreadFactorySource(AFFINITY_ENABLED);

    assertNull(source.cpuMask(null));
  }

  @Test
  void shouldReturnNullCpuMaskWhenSpecHasWrongPrefix() {
    final var source = new CpuAffinityThreadFactorySource(AFFINITY_ENABLED);

    assertNull(source.cpuMask("gpu:0"));
  }

  @Test
  void shouldReturnNullCpuMaskWhenSpecHasEmptyIndex() {
    final var source = new CpuAffinityThreadFactorySource(AFFINITY_ENABLED);

    assertNull(source.cpuMask("cpu:"));
  }

  @Test
  void shouldThrowWhenSpecIndexIsNotANumber() {
    final var source = new CpuAffinityThreadFactorySource(AFFINITY_ENABLED);

    assertThrows(RuntimeException.class, () -> source.cpuMask("cpu:abc"));
  }

  @Test
  void shouldThrowWhenSpecIndexIsOutOfRange() {
    final var source = new CpuAffinityThreadFactorySource(AFFINITY_ENABLED);

    // Only 4 effective CPUs (indexes 0-3) are available to pin to.
    assertThrows(RuntimeException.class, () -> source.cpuMask("cpu:4"));
  }

  @Test
  void shouldPinToSingleEffectiveCpuByIndex() {
    final var source = new CpuAffinityThreadFactorySource(AFFINITY_ENABLED);

    final var expectedMask = new BitSet();
    expectedMask.set(6); // effectiveCpusetCpus.get(2) == 6

    assertEquals(expectedMask, source.cpuMask("cpu:2"));
  }

  @Test
  void shouldPinToSharedCpuRangeWhenIndexIsMinusOne() {
    final var source = new CpuAffinityThreadFactorySource(AFFINITY_ENABLED);
    assertEquals(CpuAffinityThreadFactorySource.DEFAULT_AFFINITY, source.cpuMask("cpu:-1"));
  }
}
