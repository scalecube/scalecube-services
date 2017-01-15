package io.scalecube.transport;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Used to create and cache shared thread pools with given name.
 */
public class ThreadFactory {
  
  /**
   * Used to create and cache shared thread pools with given name.
   * 
   * @name the requested name of the single thread executor if not cached will be created.
   */
  public static ScheduledExecutorService newSingleScheduledExecutorService(String name) {
    String nameFormat = name.replaceAll("%", "%%");
    return compute(nameFormat);
  }
  

  private static ScheduledExecutorService compute(String name) {
    return Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat(name).setDaemon(true).build());
  }
}
