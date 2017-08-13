package io.scalecube.cluster.membership;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public final class IdGenerator {

  private static final int DEFAULT_SIZE = 10;

  /**
   * The default message digest algorithm to use if we cannot use the requested one.
   */
  protected static final String DEFAULT_ALGORITHM = "MD5";

  /**
   * The message digest algorithm to be used when generating session identifiers. This must be an algorithm supported by
   * the <code>java.security.MessageDigest</code> class on your platform.
   */
  private static String algorithm = DEFAULT_ALGORITHM;

  private static ThreadLocal<MessageDigest> digestHolder = new ThreadLocal<MessageDigest>() {
    @Override
    protected MessageDigest initialValue() {
      return getDigest();
    }
  };

  public static String getAlgorithm() {
    return algorithm;
  }

  public static void setAlgorithm(String algorithm) {
    IdGenerator.algorithm = algorithm;
  }

  public static String generateId() {
    return generateId(DEFAULT_SIZE);
  }

  public static String generateId(int length) {
    return generateId(length, ThreadLocalRandom.current());
  }

  /**
   * Generate id.
   * 
   * @param length of the id.
   * @param random random factor.
   * @return id as string.
   */
  public static String generateId(int length, Random random) {
    byte[] buffer = new byte[length];

    int resultLenCounter = 0;
    MessageDigest digest = digestHolder.get();
    int resultLen = length * 2;
    char[] result = new char[resultLen];

    while (resultLenCounter < resultLen) {
      random.nextBytes(buffer);
      buffer = digest.digest(buffer);
      for (int j = 0; j < buffer.length && resultLenCounter < resultLen; j++) {
        result[resultLenCounter++] = forHexDigit((buffer[j] & 0xf0) >> 4);
        result[resultLenCounter++] = forHexDigit(buffer[j] & 0x0f);
      }
    }

    digest.reset();
    return new String(result);
  }

  private static char forHexDigit(int digit) {
    if (digit < 10) {
      return (char) ('0' + digit);
    }
    return (char) ('A' - 10 + digit);
  }

  /**
   * Return the MessageDigest object to be used for calculating session identifiers. If none has been created yet,
   * initialize one the first time this method is called.
   * 
   * @return The hashing algorithm
   */
  private static MessageDigest getDigest() {
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance(algorithm);
    } catch (NoSuchAlgorithmException ex) {
      try {
        digest = MessageDigest.getInstance(DEFAULT_ALGORITHM);
      } catch (NoSuchAlgorithmException ex2) {
        throw new IllegalStateException("No algorithms for IdGenerator");
      }
    }
    return digest;
  }

  @Override
  public final String toString() {
    // This is to make the point that we need toString to return something
    // that includes some sort of system identifier as does the default.
    // Don't change this unless you really know what you are doing.
    return super.toString();
  }

}
