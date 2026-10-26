package io.scalecube.services.concurrent;

import java.util.BitSet;
import java.util.concurrent.ThreadFactory;
import net.openhft.affinity.Affinity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link ThreadFactorySource} that sets thread affinity to specific CPU.
 *
 * <p>Uses {@link CpusDescriptor} to resolve logical indexes to physical CPU ids. It supports {@code
 * "cpu:INDEX"} specification format, where {@code INDEX} is zero-based position of the CPU in the
 * {@link CpusDescriptor#effectiveCpusetCpus()} list.
 */
public class CpuAffinityThreadFactorySource implements ThreadFactorySource {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CpuAffinityThreadFactorySource.class);
  public static final BitSet DEFAULT_AFFINITY = Affinity.getAffinity();

  private final CpusDescriptor cpusDescriptor;

  public CpuAffinityThreadFactorySource(CpusDescriptor cpusDescriptor) {
    this.cpusDescriptor = cpusDescriptor;
    if (cpusDescriptor.effectiveCpusetCpus() == null) {
      LOGGER.warn("CPU affinity disabled: {}", cpusDescriptor);
    } else {
      LOGGER.info("CPU affinity enabled: {}", cpusDescriptor);
    }
  }

  @Override
  public ThreadFactory threadFactory(String spec) {
    final var cpuMask = cpuMask(spec);
    return cpuMask == null ? null : new CpuAffinityThreadFactory(cpuMask);
  }

  /**
   * Parses the spec and returns a bitmask suitable for {@link
   * net.openhft.affinity.Affinity#setAffinity(BitSet)}. The implementation respects
   * SHARED_CPU_COUNT and the list of CPUs eligible for pinning threads (on top of SHARED_CPU_COUNT)
   * as provided by CpusDescriptor.
   *
   * <ul>
   *   <li>If the {@code INDEX} in the spec is specified as -1, it will return a bitmask covering
   *       the CPUs that make up the SHARED_CPU_COUNT range of cpus.
   *   <li>If the {@code INDEX} in the spec is a non-negative integer, the bitmask will have only a
   *       single field set, where the index acts as a zero-based position of the CPU in the {@link
   *       CpusDescriptor#effectiveCpusetCpus()} list.
   * </ul>
   *
   * @param spec the spec
   * @return the bitmask suitable for {@link net.openhft.affinity.Affinity#setAffinity(BitSet)}, or
   *     {@code null} if the spec is unsupported or CPU affinity is disabled
   */
  BitSet cpuMask(String spec) {
    if (cpusDescriptor.effectiveCpusetCpus() == null
        || cpusDescriptor.effectiveCpusetCpus().isEmpty()) {
      return null;
    }

    if (spec == null || !spec.startsWith("cpu:")) {
      return null;
    }

    final var cpuSpec = spec.substring("cpu:".length());
    if (cpuSpec.isEmpty()) {
      return null;
    }

    try {
      int cpuIndex = Integer.parseInt(cpuSpec);
      if (cpuIndex == -1) {
        return DEFAULT_AFFINITY;
      }
      return pinnedCpu(cpuIndex);
    } catch (Exception e) {
      throw new RuntimeException(
          "Cannot get cpuId, spec: " + spec + ", cpus: " + cpusDescriptor, e);
    }
  }

  private BitSet pinnedCpu(int index) {
    final var cpuMask = new BitSet(cpusDescriptor.cpusetCpus().size());
    cpuMask.set(cpusDescriptor.effectiveCpusetCpus().get(index));
    return cpuMask;
  }
}
