package io.scalecube.testlib;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base test class.
 * 
 * @author Anton Kharenko
 */
public class BaseTest {

  protected static final Logger LOGGER = LoggerFactory.getLogger(BaseTest.class);

  @Rule
  public final TestName testName = new TestName();

  @Before
  public final void baseSetUp() throws Exception {
    LOGGER.info("***** Test started  : " + getClass().getSimpleName() + "." + testName.getMethodName() + " *****");
  }

  @After
  public final void baseTearDown() throws Exception {
    LOGGER.info("***** Test finished : " + getClass().getSimpleName() + "." + testName.getMethodName() + " *****");
  }

}
