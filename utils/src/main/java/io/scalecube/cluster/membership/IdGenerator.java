package io.scalecube.cluster.membership;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public final class IdGenerator {

  private static final int DEFAULT_SIZE = 10;

  public static final String DEFAULT_ALGORITHM = "MD5";

  public static final ThreadLocal<MessageDigest> DIGEST_HOLDER = ThreadLocal.withInitial(IdGenerator::getDigest);

  private IdGenerator() {
    // Do not instantiate
  }

  public static String generateId() {
    return generateId(DEFAULT_SIZE);
  }

  public static String generateId(int length) {
    return generateId(length, ThreadLocalRandom.current());
  }

  /**
   * Generate and return a new session identifier.
   *
   * @param length The number of bytes to generate
   * @return a new id string
   */
  public static String generateId(int length, Random random) {
    byte[] buffer = new byte[length];

    int resultLenCounter = 0;
    MessageDigest digest = DIGEST_HOLDER.get();
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
    return (digit < 10) ? (char) ('0' + digit) : (char) ('A' - 10 + digit);
  }

  /**
   * Return the MessageDigest object to be used for calculating session identifiers. If none has been created yet,
   * initialize one the first time this method is called.
   *
   * @return The hashing algorithm
   */
  private static MessageDigest getDigest() {
    try {
      return MessageDigest.getInstance(DEFAULT_ALGORITHM);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("NoSuchAlgorithmException at getting instance for " + DEFAULT_ALGORITHM);
    }
  }
}
