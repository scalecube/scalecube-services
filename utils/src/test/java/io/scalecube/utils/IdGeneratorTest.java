package io.scalecube.utils;

import io.scalecube.cluster.membership.IdGenerator;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class IdGeneratorTest {

  private final static int ATTEMPTS = (int) 2e+6;

  @Test
  public void generatorUniquenessTest() {
    Map<String, Integer> previds = new HashMap<>(ATTEMPTS);

    for (int attemptNumber = 0; attemptNumber < ATTEMPTS; attemptNumber++) {
      String id = generateId();
      if (previds.containsKey(id)) {
        Assert.fail("Found key duplication on attempt " + attemptNumber +
            " same id = " + id + " as at attempt " + previds.get(id));
      } else {
        previds.put(id, attemptNumber);
      }
    }
  }

  private String generateId() {
    return IdGenerator.generateId(10);
  }
}
