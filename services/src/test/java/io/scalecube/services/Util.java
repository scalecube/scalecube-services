package io.scalecube.services;

import java.util.concurrent.CountDownLatch;

public class Util {

  public static int sleep(int i, int ms, int every) {
    if (i % every == 0) {
      sleep(ms);
    }
    return ms;
  }

  public static void sleep(int ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  public static void drainLatch(int count, CountDownLatch countLatch) {
    for (int j = 0; j < count; j++) {
      countLatch.countDown();
    }
  }
}
