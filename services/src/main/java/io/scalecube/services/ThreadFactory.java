package io.scalecube.services;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Used to create and cache shared thread pools with given name.
 */
public class ThreadFactory {

  private static final ConcurrentMap<String, ScheduledExecutorService> schedulers = new ConcurrentHashMap<>();
  public static String SC_SERVICES_TIMEOUT = "sc-services-timeout";

  /**
   * Used to create and cache shared thread pools with given name.
   * 
   * @param name the requested name of the single thread executor if not cached will be created.
   * @return computed or existing scheduled executor
   */
  public static ScheduledExecutorService singleScheduledExecutorService(String name) {
    return schedulers.computeIfAbsent(name, ThreadFactory::compute);
  }

  private static ScheduledExecutorService compute(String name) {
    return Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat(name).setDaemon(true).build());
  }

}
