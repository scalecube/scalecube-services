package io.servicefabric.testlib;

import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Base test class.
 * @author Anton Kharenko
 */
public class BaseTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseTest.class);

  @Rule
  public final TestName testName = new TestName();

  public static void assertAmongExpectedClasses(Class actualClass, Class... expectedClasses) {
    for (Class expectedClass : expectedClasses) {
      if (expectedClass.equals(actualClass)) {
        return;
      }
    }
    fail("Expected classes " + Arrays.toString(expectedClasses) + ", but actual: " + actualClass);
  }

  @Before
  public final void baseSetUp() throws Exception {
    LOGGER.info("********** Test started: " + getClass().getSimpleName() + "." + testName.getMethodName()
        + " **********");
  }

  @After
  public final void baseTearDown() throws Exception {
    LOGGER.info(
        "********** Test finished: " + getClass().getSimpleName() + "." + testName.getMethodName() + " **********");
  }

}
