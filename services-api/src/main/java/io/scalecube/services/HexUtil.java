package io.scalecube.services;

public final class HexUtil {

  private HexUtil() {
    // Do not instantiate
  }

  /**
   * Converts bytes array to hex string.
   *
   * @param bytes bytes array
   * @return hex string
   */
  public static String toHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }
}
