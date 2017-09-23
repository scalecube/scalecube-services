package io.scalecube.transport;

import static io.scalecube.transport.Preconditions.checkArgument;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nullable;

public class Strings {

  static boolean isNullOrEmpty(@Nullable String string) {
    return string == null || string.isEmpty();
  }

  public static void checkNotNullOrEmpty(String string) {
    checkArgument(!isNullOrEmpty(string), "String is null or empty!");
  }

  public static void checkNotNullOrEmpty(String string, @Nullable Object errorMessage) {
    checkArgument(!isNullOrEmpty(string), errorMessage);
  }

  public static Set<String> asSet(String... strings ) {
    return new HashSet<String>(Arrays.asList(strings));
  }
}
