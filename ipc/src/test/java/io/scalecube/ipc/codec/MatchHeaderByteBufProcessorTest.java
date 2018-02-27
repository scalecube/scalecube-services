package io.scalecube.ipc.codec;

import static io.scalecube.ipc.codec.JsonCodec.ASCII_CLOSING_BRACE;
import static io.scalecube.ipc.codec.JsonCodec.ASCII_DOUBLE_QUOTES;
import static io.scalecube.ipc.codec.JsonCodec.ASCII_ESCAPE;
import static io.scalecube.ipc.codec.JsonCodec.ASCII_OPENING_BRACE;
import static io.scalecube.ipc.codec.JsonCodec.MatchHeaderByteBufProcessor.STATE_ESCAPED;
import static io.scalecube.ipc.codec.JsonCodec.MatchHeaderByteBufProcessor.STATE_OBJECT;
import static io.scalecube.ipc.codec.JsonCodec.MatchHeaderByteBufProcessor.STATE_START;
import static io.scalecube.ipc.codec.JsonCodec.MatchHeaderByteBufProcessor.STATE_STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class MatchHeaderByteBufProcessorTest {

  private JsonCodec.MatchHeaderByteBufProcessor processor;

  private int startIndex = 10;

  @Before
  public void init() {
    processor = JsonCodec.MatchHeaderByteBufProcessor.newInstance(startIndex);
  }

  @Test
  public void shouldChangeStateWtihoutEscapeWhenWasEscaped() {
    processor.setState(STATE_STRING | STATE_ESCAPED);
    assertTrue(processor.process(ASCII_DOUBLE_QUOTES));
    assertEquals(STATE_STRING, processor.getState());
  }

  @Test
  public void shouldUpdateLeftIndexWhenFirstOpenBraces() {
    assertTrue(processor.process(ASCII_OPENING_BRACE));
    assertEquals(startIndex, processor.getLeftBraces());
    assertTrue((processor.getState() & STATE_OBJECT) != 0);
  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowExceptionWhehEscapeNotInString() {
    processor.process(ASCII_ESCAPE);
  }

  @Test
  public void shouldMoveToStringStateWhenUnescapedDoubleQuotes() {
    processor.process(ASCII_DOUBLE_QUOTES);
    assertTrue((processor.getState() & STATE_STRING) != 0);
  }

  @Test
  public void shouldNotCountBracesWhenStringState() {
    assertTrue((processor.getState() & STATE_START) != 0);
    processor.process(ASCII_OPENING_BRACE);
    assertTrue((processor.getState() & STATE_OBJECT) != 0);
    processor.process(ASCII_DOUBLE_QUOTES);
    assertTrue((processor.getState() & STATE_STRING) != 0);
    int oldBracesCounter = processor.getBracesCounter();
    assertTrue(processor.process(ASCII_OPENING_BRACE));
    assertEquals(oldBracesCounter, processor.getBracesCounter());
  }

  @Test
  public void shouldIncrementCounterWhenOpenBraces() {
    assertTrue((processor.getState() & STATE_START) != 0);
    processor.process(ASCII_OPENING_BRACE);
    assertTrue((processor.getState() & STATE_OBJECT) != 0);
    int oldBracesCounter = processor.getBracesCounter();
    assertTrue(processor.process(ASCII_OPENING_BRACE));
    assertEquals(oldBracesCounter + 1, processor.getBracesCounter());
  }

  @Test
  public void shouldIncrementCounterWhenCloseBraces() {
    assertTrue((processor.getState() & STATE_START) != 0);
    processor.process(ASCII_OPENING_BRACE);
    assertTrue((processor.getState() & STATE_OBJECT) != 0);
    int oldBracesCounter = processor.getBracesCounter();
    processor.process(ASCII_CLOSING_BRACE);
    assertEquals(oldBracesCounter - 1, processor.getBracesCounter());
  }

  @Test
  public void shouldReturnFalseWhenObjectAndBracesCounter0() {
    assertTrue((processor.getState() & STATE_START) != 0);
    processor.process(ASCII_OPENING_BRACE);
    assertTrue((processor.getState() & STATE_OBJECT) != 0);
    int oldBracesCounter = processor.getBracesCounter();
    assertFalse(processor.process(ASCII_CLOSING_BRACE));
    assertEquals(oldBracesCounter - 1, processor.getBracesCounter());
  }
}
