package io.scalecube.examples.util;

public class Util {

  public static void sleepForever() {
    while (true) {
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
}
