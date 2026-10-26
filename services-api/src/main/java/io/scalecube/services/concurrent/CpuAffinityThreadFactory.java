package io.scalecube.services.concurrent;

import java.util.BitSet;
import java.util.concurrent.ThreadFactory;
import net.openhft.affinity.Affinity;
import org.agrona.LangUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CpuAffinityThreadFactory implements ThreadFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(CpuAffinityThreadFactory.class);

  private final BitSet cpuMask;

  public CpuAffinityThreadFactory(BitSet cpuMask) {
    this.cpuMask = cpuMask;
  }

  @Override
  public Thread newThread(Runnable r) {
    return new Thread(
        () -> {
          try {
            Affinity.setAffinity(cpuMask);
            LOGGER.info("Thread pinned to CPU {}", cpuMask);
          } catch (Exception e) {
            LOGGER.warn("Cannot pin thread to CPU {}", cpuMask, e);
            LangUtil.rethrowUnchecked(e);
          }
          r.run();
        });
  }
}
