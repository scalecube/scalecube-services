package io.scalecube.services.utils;

import java.lang.reflect.Method;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingExtension
    implements AfterEachCallback, BeforeEachCallback, AfterAllCallback, BeforeAllCallback {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoggingExtension.class);

  @Override
  public void beforeAll(ExtensionContext context) {
    LOGGER.info(
        "***** Setup: " + context.getTestClass().map(Class::getSimpleName).orElse("") + " *****");
  }

  @Override
  public void afterEach(ExtensionContext context) {
    LOGGER.info(
        "***** Test finished: "
            + context.getTestClass().map(Class::getSimpleName).orElse("")
            + "."
            + context.getTestMethod().map(Method::getName).orElse("")
            + "."
            + context.getDisplayName()
            + " *****");
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    LOGGER.info(
        "***** Test started: "
            + context.getTestClass().map(Class::getSimpleName).orElse("")
            + "."
            + context.getTestMethod().map(Method::getName).orElse("")
            + "."
            + context.getDisplayName()
            + " *****");
  }

  @Override
  public void afterAll(ExtensionContext context) {
    LOGGER.info(
        "***** TearDown: "
            + context.getTestClass().map(Class::getSimpleName).orElse("")
            + " *****");
  }
}
