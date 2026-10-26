package io.scalecube.services.concurrent;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringJoiner;

/**
 * Describes CPU resources available to the process.
 *
 * <p>This class identifies physical CPUs allocated to current cgroup by reading the {@code
 * cpuset.cpus.effective} system file. It accounts for shared CPU pool (CPUs reserved for general
 * system tasks) to calculate the {@code effectiveCpusetCpus} available for dedicated application
 * threads.
 *
 * <p>The effective CPU list is derived by skipping the first {@code N} CPUs (defined by {@code
 * SHARED_CPU_COUNT}) from the total allocated set.
 */
public class CpusDescriptor {

  private static final String SHARED_CPU_COUNT_PROP_NAME = "SHARED_CPU_COUNT";
  private static final String CGROUP_CPUSET_PATH = "/sys/fs/cgroup/cpuset.cpus.effective";

  private final int sharedCpuCount;
  private final List<Integer> cpusetCpus;
  private final List<Integer> effectiveCpusetCpus;

  public CpusDescriptor(
      int sharedCpuCount, List<Integer> cpusetCpus, List<Integer> effectiveCpusetCpus) {
    this.sharedCpuCount = sharedCpuCount;
    this.cpusetCpus = cpusetCpus;
    this.effectiveCpusetCpus = effectiveCpusetCpus;
  }

  public static CpusDescriptor obtain() {
    final var properties = new Properties();
    properties.putAll(System.getenv());
    properties.putAll(System.getProperties());
    return CpusDescriptor.from(properties, Path.of(CGROUP_CPUSET_PATH));
  }

  public static CpusDescriptor from(Properties properties, Path cpusetPath) {
    final var sharedCpuCount =
        Integer.parseInt(properties.getProperty(SHARED_CPU_COUNT_PROP_NAME, "0"));
    final var cpusetCpus = getCpusetCpus(cpusetPath);

    if (sharedCpuCount > 0 && cpusetCpus != null) {
      final var effectiveCpuIds = cpusetCpus.stream().skip(sharedCpuCount).toList();
      if (!effectiveCpuIds.isEmpty()) {
        return new CpusDescriptor(sharedCpuCount, cpusetCpus, effectiveCpuIds);
      }
    }

    return new CpusDescriptor(sharedCpuCount, cpusetCpus, null);
  }

  public int sharedCpuCount() {
    return sharedCpuCount;
  }

  public List<Integer> cpusetCpus() {
    return cpusetCpus;
  }

  public List<Integer> effectiveCpusetCpus() {
    return effectiveCpusetCpus;
  }

  private static List<Integer> getCpusetCpus(Path path) {
    if (!Files.exists(path)) {
      return null;
    }
    try {
      final var cpusSpec = Files.readString(path).trim();
      return cpusSpec.isBlank() ? null : parseCpusSpec(cpusSpec);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static List<Integer> parseCpusSpec(String spec) {
    final var cpuIds = new ArrayList<Integer>();

    // Format can be "0-2,4,7-8"
    for (var part : spec.split(",")) {
      if (part.contains("-")) {
        final var range = part.split("-");
        final var start = Integer.parseInt(range[0]);
        final var end = Integer.parseInt(range[1]);
        for (int i = start; i <= end; i++) {
          cpuIds.add(i);
        }
      } else if (!part.isBlank()) {
        cpuIds.add(Integer.parseInt(part));
      }
    }

    cpuIds.sort(Integer::compareTo);
    return cpuIds;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", CpusDescriptor.class.getSimpleName() + "[", "]")
        .add("sharedCpuCount=" + sharedCpuCount)
        .add("cpusetCpus=" + cpusetCpus)
        .add("effectiveCpusetCpus=" + effectiveCpusetCpus)
        .toString();
  }
}
