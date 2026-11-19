package io.scalecube.services.gateway;

import io.netty.buffer.ByteBuf;

public class JsonMasker {

  /**
   * Masks values for the given key in the JSON buffer.
   *
   * @param buffer The buffer containing JSON data.
   * @param offset Start position in the buffer.
   * @param length Number of bytes to scan starting from offset.
   * @param keyBytes The key to search for (e.g. "password".getBytes()).
   * @param fullMask If true, masks the entire value. If false, keeps first/last 2 chars visible for
   *     values >= 6 chars.
   */
  public static void mask(
      ByteBuf buffer, int offset, int length, byte[] keyBytes, boolean fullMask) {
    final var limit = offset + length;
    int currentIndex = offset;

    while (currentIndex < limit) {
      // 1. Find match for key
      int keyStart = findMatch(buffer, currentIndex, limit, keyBytes);
      if (keyStart == -1) {
        break;
      }

      // --- BOUNDARY CHECKS ---
      boolean isAtStart = (keyStart == offset);
      boolean isPrecededByQuote = (keyStart > offset) && (buffer.getByte(keyStart - 1) == '"');

      if (!isAtStart && !isPrecededByQuote) {
        currentIndex = keyStart + 1;
        continue;
      }

      int keyEnd = keyStart + keyBytes.length;
      if (keyEnd >= limit || buffer.getByte(keyEnd) != '"') {
        currentIndex = keyStart + 1;
        continue;
      }

      // 2. Find colon
      int colonIndex = findNextChar(buffer, keyEnd, limit, (byte) ':');
      if (colonIndex == -1) {
        break;
      }

      // 3. Find START of value
      int valueStart = findValueStart(buffer, colonIndex + 1, limit);
      if (valueStart == -1) {
        currentIndex = colonIndex + 1;
        continue;
      }

      final byte first = buffer.getByte(valueStart);
      if (first == '{' || first == '[') {
        // OBJECT OR ARRAY VALUE: mask everything inside, keep the JSON structure
        final int end = maskContainer(buffer, valueStart, limit);
        if (end >= limit) {
          break;
        }
        currentIndex = end;
        continue;
      }

      int contentStart;
      int contentEnd;
      int searchResumeIndex;

      if (first == '"') {
        // QUOTED VALUE
        contentStart = valueStart + 1; // Skip opening quote
        int closeQuote = findClosingQuote(buffer, contentStart, limit);

        if (closeQuote == -1) {
          contentEnd = limit; // Handle truncated
          searchResumeIndex = limit;
        } else {
          contentEnd = closeQuote; // Stop before closing quote
          searchResumeIndex = closeQuote + 1;
        }
      } else {
        // UNQUOTED VALUE
        contentStart = valueStart;
        contentEnd = findUnquotedValueEnd(buffer, contentStart, limit);
        searchResumeIndex = contentEnd;
      }

      // 4. Apply Masking
      applyMask(buffer, contentStart, contentEnd, fullMask);

      if (searchResumeIndex >= limit) {
        break;
      }
      currentIndex = searchResumeIndex;
    }
  }

  private static void applyMask(ByteBuf buffer, int start, int end, boolean fullMask) {
    int length = end - start;
    if (length <= 0) {
      return;
    }

    if (fullMask || length < 6) {
      for (int i = start; i < end; i++) {
        buffer.setByte(i, (byte) '*');
      }
    } else {
      // Partial mask: Keep first 2 and last 2 visible
      int maskStart = start + 2;
      int maskEnd = end - 2;

      for (int i = maskStart; i < maskEnd; i++) {
        buffer.setByte(i, (byte) '*');
      }
    }
  }

  /**
   * Masks a JSON object or array value that starts at {@code start}: every character of its strings
   * and every bare value, keeping quotes, brackets, ':', ',' and whitespace.
   *
   * @return index right after the closing bracket, or {@code limit} if the value is truncated
   */
  private static int maskContainer(ByteBuf buffer, int start, int limit) {
    int depth = 0;
    boolean inString = false;
    boolean escaped = false;
    for (int i = start; i < limit; i++) {
      final byte b = buffer.getByte(i);
      if (inString) {
        if (escaped) {
          escaped = false;
        } else if (b == '\\') {
          escaped = true;
        } else if (b == '"') {
          inString = false;
          continue;
        }
        buffer.setByte(i, '*');
      } else if (b == '"') {
        inString = true;
      } else if (b == '{' || b == '[') {
        depth++;
      } else if (b == '}' || b == ']') {
        if (--depth == 0) {
          return i + 1;
        }
      } else if (b != ':' && b != ',' && b != ' ' && b != '\t' && b != '\n' && b != '\r') {
        buffer.setByte(i, '*');
      }
    }
    return limit;
  }

  private static int findMatch(ByteBuf buffer, int start, int limit, byte[] pattern) {
    for (int i = start; i <= limit - pattern.length; i++) {
      if (matches(buffer, i, pattern)) {
        return i;
      }
    }
    return -1;
  }

  private static boolean matches(ByteBuf buffer, int index, byte[] pattern) {
    for (int j = 0; j < pattern.length; j++) {
      if (buffer.getByte(index + j) != pattern[j]) {
        return false;
      }
    }
    return true;
  }

  private static int findNextChar(ByteBuf buffer, int start, int limit, byte target) {
    for (int i = start; i < limit; i++) {
      if (buffer.getByte(i) == target) {
        return i;
      }
    }
    return -1;
  }

  private static int findValueStart(ByteBuf buffer, int start, int limit) {
    for (int i = start; i < limit; i++) {
      byte b = buffer.getByte(i);
      if (b != ' ' && b != '\t' && b != '\n' && b != '\r') {
        return i;
      }
    }
    return -1;
  }

  private static int findClosingQuote(ByteBuf buffer, int start, int limit) {
    boolean escaped = false;
    for (int i = start; i < limit; i++) {
      byte b = buffer.getByte(i);
      if (escaped) {
        escaped = false;
      } else if (b == '\\') {
        escaped = true;
      } else if (b == '"') {
        return i;
      }
    }
    return -1;
  }

  private static int findUnquotedValueEnd(ByteBuf buffer, int start, int limit) {
    for (int i = start; i < limit; i++) {
      byte b = buffer.getByte(i);
      if (b == ',' || b == '}' || b == ']' || b == '"' || b == ' ' || b == '\t' || b == '\n'
          || b == '\r') {
        return i;
      }
    }
    return limit;
  }
}
