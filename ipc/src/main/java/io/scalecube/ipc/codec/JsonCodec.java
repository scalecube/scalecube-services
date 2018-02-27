package io.scalecube.ipc.codec;

import static com.google.common.base.Preconditions.checkState;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ByteProcessor;
import io.netty.util.Recycler;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

public final class JsonCodec {

  public static final byte ASCII_COLON = 58; // :
  public static final byte ASCII_DOUBLE_QUOTES = 34; // "
  public static final byte ASCII_OPENING_BRACE = 123; // {
  public static final byte ASCII_CLOSING_BRACE = 125; // }
  public static final byte ASCII_COMMA = 44; // ,
  public static final byte ASCII_ESCAPE = 92; // \

  // Also allowed to be escaped in string vvalues
  public static final byte ASCII_SLASH = 47; // / (solidus)
  public static final byte ASCII_BACKSPACE = 8; // backspace
  public static final byte ASCII_FORM_FEED = 12; // form feed

  // Insignificant whitespaces
  public static final byte ASCII_WHITE_SPACE = 32; // Space
  public static final byte ASCII_HORIZONTAL_TAB = 9; // Horizontal tab
  public static final byte ASCII_NEW_LINE = 10; // Line feed or New line
  public static final byte ASCII_CR = 13; // Carriage return
  public static final byte ASCII_U_HEX = 117; // Escape any hex symbol

  private JsonCodec() {
    // Do not instantiate
  }

  /**
   * @param sourceBuf source buffer.
   * @param getList fields for getting values for (String objects will be allocated).
   * @param matchList fields to match, i.e. not to allocate String (sliced buffers will be created).
   * @param consumer a callback consumer accepting headerName and either String or ByteBuf.
   */
  public static void decode(ByteBuf sourceBuf, List<String> getList, List<String> matchList,
      BiConsumer<String, Object> consumer) {

    List<String> get = new ArrayList<>(getList);
    List<String> match = new ArrayList<>(matchList);

    boolean seekingNextObj = false;
    int leftQuotes = -1;
    int rightQuotes = -1;
    while (sourceBuf.readableBytes() > 0) {
      if (get.isEmpty() && match.isEmpty()) {
        return;
      }

      byte colon = sourceBuf.readByte();
      if (seekingNextObj) {
        if (!isSignificantSymbol(colon)) {
          continue;
        }
        checkState(colon == ASCII_COMMA || colon == ASCII_CLOSING_BRACE, "Invalid JSON request");
        seekingNextObj = false;
      }
      if (colon == ASCII_DOUBLE_QUOTES) { // match ASCII double quotes
        if (leftQuotes == -1) {
          leftQuotes = sourceBuf.readerIndex();
        } else {
          rightQuotes = sourceBuf.readerIndex() - 1;
        }
        continue;
      }

      if (leftQuotes != -1 && rightQuotes != -1) { // if two double quotes were matched -- match following colon
        if (!isSignificantSymbol(colon)) {
          continue;
        }

        if (colon == ASCII_COLON) {
          ByteBuf bbSlice = sourceBuf.slice(leftQuotes, rightQuotes - leftQuotes);
          int index;
          if ((index = headerInd(get, bbSlice)) >= 0) {
            String headerName = get.remove(index);
            consumer.accept(headerName, getFlatHeaderValue(sourceBuf, headerName));
          } else if ((index = headerInd(match, bbSlice)) >= 0) {
            String headerName = match.remove(index);
            consumer.accept(headerName, matchHeaderValue(sourceBuf));
          } else {
            skipValue(sourceBuf);
          }
        }
        seekingNextObj = true;
        leftQuotes = -1;
        rightQuotes = -1;
      }
    }
  }

  /**
   * @param targetBuf source outgoing buffer.
   * @param flatList 'flat' fields (e.g. "streamId") to go into resulting json.
   * @param complexList 'complex' fields (e.g. field "data") to go into resulting json.
   * @param mapper function which by header name return corresponding object (either String or ByteBuf).
   */
  public static void encode(ByteBuf targetBuf, List<String> flatList, List<String> complexList,
      Function<String, Object> mapper) {
    // writeStartObject
    targetBuf.writeByte(ASCII_OPENING_BRACE);

    boolean flatValueHasBeenWritten = false;

    // generic message headers
    for (int i = 0; i < flatList.size(); i++) {
      String field = flatList.get(i);
      Object value = mapper.apply(field);
      if (value != null) {
        if (i > 0) {
          targetBuf.writeByte(ASCII_COMMA);
        }
        writeCharSequence(targetBuf, field); // field
        targetBuf.writeByte(ASCII_COLON);
        writeCharSequence(targetBuf, String.valueOf(value)); // value
        flatValueHasBeenWritten = true;
      }
    }

    // write data
    for (int i = 0; i < complexList.size(); i++) {
      String field = complexList.get(i);
      Object value = mapper.apply(field);
      // at this point it's assumed 'buffer' holds already a valid JSON object
      if (value != null && ((ByteBuf) value).isReadable()) {
        if (flatValueHasBeenWritten || i > 0) {
          targetBuf.writeByte(ASCII_COMMA);
        }
        writeCharSequence(targetBuf, field); // field
        targetBuf.writeByte(ASCII_COLON);
        targetBuf.writeBytes(((ByteBuf) value).slice());
      }
    }

    // writeEndObject
    targetBuf.writeByte(ASCII_CLOSING_BRACE);
  }

  private static int[] skipValue(ByteBuf bb) {
    int[] result = {-1, -1};
    MatchHeaderByteBufProcessor processor = MatchHeaderByteBufProcessor.newInstance(bb.readerIndex());
    try {
      bb.forEachByte(processor);
      checkState(processor.bracesCounter == 0, "Invalid JSON: curly braces are incorrect");
      bb.readerIndex(processor.index);
      result[0] = processor.leftBraces;
      result[1] = processor.index - processor.leftBraces;
    } finally {
      processor.recycle();
    }
    return result;
  }

  private static int headerInd(List<String> headers, ByteBuf bbSlice) {
    for (int i = 0; i < headers.size(); i++) {
      if (headerMatched(headers.get(i), bbSlice)) {
        return i;
      }
    }
    return -1;
  }

  private static boolean headerMatched(String header, ByteBuf bbSlice) {
    boolean matched = bbSlice.capacity() == header.length();
    if (matched) {
      StringMatchProcessor processor = StringMatchProcessor.newInstance(header);
      try {
        if (bbSlice.forEachByte(processor) > -1) {
          return false;
        }
      } finally {
        processor.recycle();
      }
    }
    return matched;
  }

  /**
   * @return allocated utf-8 String object.
   */
  private static String getFlatHeaderValue(ByteBuf bb, String headerName) {
    GetHeaderProcessor processor = GetHeaderProcessor.newInstance(bb.readerIndex());
    String result;
    try {
      bb.forEachByte(processor);
      checkState(processor.firstQuotes != -1,
          "Invalid JSON: can't find value opening quotes, headerName=" + headerName);
      checkState(processor.firstQuotes != processor.quotes,
          "Invalid JSON: value closing quotes are missing, headerName=" + headerName);
      bb.readerIndex(processor.readerIndex);
      result = processor.targetBuf.toString(StandardCharsets.UTF_8);
    } finally {
      if (processor != null) {
        processor.recycle();
      }
    }
    return result;
  }

  /**
   * @return sliced ByteBuf object encapsulated header value.
   */
  private static ByteBuf matchHeaderValue(ByteBuf bb) {
    int[] matchedValueBounds = skipValue(bb);
    checkState(matchedValueBounds[1] > -1, "Invalid JSON request");
    return bb.slice(matchedValueBounds[0], matchedValueBounds[1]);
  }

  /**
   * Check if the character with specified code (ASCII) is allowed to be escaped inside JSON string.
   * 
   * @param character that might be escaped.
   * @return true if escaped.
   */
  private static boolean mightBeEscaped(byte character) {
    // TODO: currently - do not allow escaping of sequence like "\uABCD", which is possible for JSON
    return character == ASCII_DOUBLE_QUOTES
        || character == ASCII_ESCAPE
        || character == ASCII_SLASH
        || character == ASCII_BACKSPACE
        || character == ASCII_FORM_FEED
        || character == ASCII_NEW_LINE
        || character == ASCII_CR
        || character == ASCII_U_HEX
        || character == ASCII_HORIZONTAL_TAB;
  }

  /**
   * Check if the byte is Significant Symbol.
   * 
   * @param symbol - code of symbol to check
   * @return {@code true} if the symbol with code {@code b} (utf-8) is significant according to JSON spec
   */
  @SuppressWarnings("BooleanMethodIsAlwaysInverted")
  private static boolean isSignificantSymbol(byte symbol) {
    return symbol != ASCII_WHITE_SPACE
        && symbol != ASCII_HORIZONTAL_TAB
        && symbol != ASCII_NEW_LINE
        && symbol != ASCII_CR;
  }

  public static void writeCharSequence(ByteBuf bb, String value) {
    bb.writeByte(ASCII_DOUBLE_QUOTES);
    bb.writeCharSequence(value, StandardCharsets.UTF_8);
    bb.writeByte(ASCII_DOUBLE_QUOTES);
  }

  public static class MatchHeaderByteBufProcessor implements ByteProcessor {

    private static final Recycler<MatchHeaderByteBufProcessor> RECYCLER = new Recycler<MatchHeaderByteBufProcessor>() {
      @Override
      protected MatchHeaderByteBufProcessor newObject(Handle<MatchHeaderByteBufProcessor> handle) {
        return new MatchHeaderByteBufProcessor(handle);
      }
    };

    public static final int STATE_START = 1;
    public static final int STATE_OBJECT = 2;
    public static final int STATE_ESCAPED = 4;
    public static final int STATE_STRING = 8;

    private int leftBraces = -1;
    private int bracesCounter = 0;
    private int index = 0;
    private int state = STATE_START;
    private final Recycler.Handle<MatchHeaderByteBufProcessor> handle;

    private MatchHeaderByteBufProcessor(Recycler.Handle<MatchHeaderByteBufProcessor> handle) {
      this.handle = handle;
    }

    public static MatchHeaderByteBufProcessor newInstance(int readerIndex) {
      MatchHeaderByteBufProcessor matchHeaderByteBufProcessor = RECYCLER.get();
      matchHeaderByteBufProcessor.index = readerIndex;
      return matchHeaderByteBufProcessor;
    }

    public void setState(int state) {
      this.state = state;
    }

    public int getState() {
      return state;
    }

    public int getLeftBraces() {
      return leftBraces;
    }

    public void setLeftBraces(int leftBraces) {
      this.leftBraces = leftBraces;
    }

    public int getBracesCounter() {
      return bracesCounter;
    }

    public void setBracesCounter(int bracesCounter) {
      this.bracesCounter = bracesCounter;
    }

    public int getIndex() {
      return index;
    }

    public void setIndex(int index) {
      this.index = index;
    }

    private void recycle() {
      state = STATE_START;
      index = 0;
      leftBraces = -1;
      bracesCounter = -1;
      handle.recycle(this);
    }

    @Override
    public boolean process(byte value) {
      if ((state & STATE_ESCAPED) != 0) {
        state ^= STATE_ESCAPED;
        index++;
        return true;
      }
      if ((state & STATE_START) != 0 && value == ASCII_OPENING_BRACE) { // match first opening curly brace
        bracesCounter = 1;
        leftBraces = index;
        state = STATE_OBJECT;
      } else if ((state & STATE_START) != 0 && value == ASCII_DOUBLE_QUOTES) {
        bracesCounter = 1;
        leftBraces = index;
        state = STATE_STRING;
      } else {
        boolean isNotString = (state & STATE_STRING) == 0;
        if (value == ASCII_ESCAPE) {
          checkState(!isNotString, "Invalid JSON: escape symbol present outside of string");
          state |= STATE_ESCAPED;
          index++;
          return true;
        }
        if (value == ASCII_DOUBLE_QUOTES) {
          if ((state & STATE_OBJECT) != 0) {
            state ^= STATE_STRING;
          } else if (state == STATE_STRING) {
            bracesCounter--;
          }
        }
        if (value == ASCII_OPENING_BRACE && isNotString) {
          bracesCounter++;
        } else if (value == ASCII_CLOSING_BRACE && isNotString) {
          bracesCounter--;
        }
      }
      index++;
      // should return false if processing is finished. Return false only when inside object and braces counter is equal
      // 0
      boolean terminateCondition = (state == STATE_OBJECT || state == STATE_STRING) && bracesCounter == 0;

      return !terminateCondition;
    }
  }

  private static class StringMatchProcessor implements ByteProcessor {

    private static final Recycler<StringMatchProcessor> RECYCLER = new Recycler<StringMatchProcessor>() {
      @Override
      protected StringMatchProcessor newObject(Handle<StringMatchProcessor> handle) {
        return new StringMatchProcessor(handle);
      }
    };

    private String stringToMatch;
    private int index;
    private final Recycler.Handle<StringMatchProcessor> handle;

    private StringMatchProcessor(Recycler.Handle<StringMatchProcessor> handle) {
      this.handle = handle;
    }

    private static StringMatchProcessor newInstance(String stringToMatch) {
      StringMatchProcessor stringMatchProcessor = RECYCLER.get();
      stringMatchProcessor.stringToMatch = stringToMatch;
      stringMatchProcessor.index = 0;
      return stringMatchProcessor;
    }

    private void recycle() {
      handle.recycle(this);
    }

    @Override
    public boolean process(byte value) {
      return value == stringToMatch.charAt(index++);
    }
  }

  private static class GetHeaderProcessor implements ByteProcessor {

    private static final Recycler<GetHeaderProcessor> RECYCLER = new Recycler<GetHeaderProcessor>() {
      @Override
      protected GetHeaderProcessor newObject(Handle<GetHeaderProcessor> handle) {
        return new GetHeaderProcessor(handle);
      }
    };

    private int firstQuotes = -1;
    private int quotes = -1;
    private boolean inEscapeState = false;
    private boolean isParsingHex = false;
    private byte[] parsingHex = new byte[4];
    private boolean isParsingString = false;
    private int escapedHexCounter = 0;
    private ByteBuf targetBuf;
    private int readerIndex;
    private final Recycler.Handle<GetHeaderProcessor> handle;

    private GetHeaderProcessor(Recycler.Handle<GetHeaderProcessor> handle) {
      this.handle = handle;
    }

    private static GetHeaderProcessor newInstance(int readerIndex) {
      GetHeaderProcessor getHeaderProcessor = RECYCLER.get();
      getHeaderProcessor.readerIndex = readerIndex;
      getHeaderProcessor.firstQuotes = -1;
      getHeaderProcessor.quotes = -1;
      getHeaderProcessor.inEscapeState = false;
      getHeaderProcessor.isParsingString = false;
      getHeaderProcessor.isParsingHex = false;
      getHeaderProcessor.escapedHexCounter = 0;
      getHeaderProcessor.targetBuf = ByteBufAllocator.DEFAULT.buffer();
      return getHeaderProcessor;
    }

    private void recycle() {
      targetBuf.release();
      handle.recycle(this);
    }

    @Override
    public boolean process(byte ch) {
      readerIndex++; // increase every iteration, use (reader - 1) value
      // skip insignificant chars before quotes
      if (!isParsingString && !isSignificantSymbol(ch)) {
        return true;
      }
      // consume escaped symbol
      if (ch == ASCII_ESCAPE && !inEscapeState) {
        checkState(isParsingString, "Invalid JSON: got '\\' before first opening quotes");
        inEscapeState = true;
        return true; // ignore rest loop block
      }
      if (ch == ASCII_DOUBLE_QUOTES && !inEscapeState) { // match non-escaped ASCII double double quotes
        quotes = readerIndex - 1; // .. always track last non-escaped double quote
        if (!isParsingString) {
          firstQuotes = quotes; // save first double quotes
          isParsingString = true;
          return true;
        } else {
          return false; // terminate the parsing since the string is over
        }
      }
      // check if the character might be escaped and resed 'escaped' state
      if (inEscapeState) {
        checkState(mightBeEscaped(ch), "Invalid JSON: the character " + ch + " might not be escaped");
        if (ch == ASCII_U_HEX) {
          isParsingHex = true;
          inEscapeState = false;
          return true;
        }
        inEscapeState = false;
      }

      // parse hex (should be at bottom, since modifying char of result)
      if (isParsingHex) {
        checkState((ch >= 48 && ch <= 57) || (ch >= 65 && ch <= 70) || (ch >= 97 && ch <= 102)); // check if char is hex
        // symbol [0-1][A-F]
        parsingHex[escapedHexCounter++] = ch;
        if (escapedHexCounter == 4) {
          isParsingHex = false; // not parsing \\uXXXX anymore

          int index = Integer.parseInt(new String(parsingHex, StandardCharsets.UTF_8), 16);
          if (index < 0x80) { // 1 byte
            targetBuf.writeByte(index);
          } else if (index < 0x800) { // 2 bytes
            targetBuf.writeByte(0xc0 | (index >> 6));
            targetBuf.writeByte(0x80 | (index & 0x3f));
          } else if (index < 0xd800) { // 3 bytes
            targetBuf.writeByte(0xe0 | (index >> 12));
            targetBuf.writeByte(0x80 | ((index >> 6) & 0x3f));
            targetBuf.writeByte(0x80 | (index & 0x3f));
          } else {
            throw new IllegalStateException("Invalid JSON: surrogate symbols are not supported");
          }
          escapedHexCounter = 0;
        }
        return true;
      }
      targetBuf.writeByte(ch);
      return true;
    }
  }
}
