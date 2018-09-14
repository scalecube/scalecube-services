package io.scalecube.services;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseTest {

  protected static final Logger LOGGER = LoggerFactory.getLogger(BaseTest.class);

  /**
   * Setup.
   *
   * @param testInfo test info
   */
  @BeforeEach
  public final void baseSetUp(TestInfo testInfo) {
    LOGGER.info(
        "***** Test started  : "
            + getClass().getSimpleName()
            + "."
            + testInfo.getDisplayName()
            + " *****");
  }

  /**
   * Setup.
   *
   * @param testInfo test info
   */
  @AfterEach
  public final void baseTearDown(TestInfo testInfo) {
    LOGGER.info(
        "***** Test finished : "
            + getClass().getSimpleName()
            + "."
            + testInfo.getDisplayName()
            + " *****");
  }
}
