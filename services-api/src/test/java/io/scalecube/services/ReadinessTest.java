package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class ReadinessTest {

  @Test
  void testAllConditionsMet() throws Exception {
    final var cond1 = new AtomicBoolean(false);
    final var cond2 = new AtomicBoolean(false);

    final var c1 =
        new Readiness.ReadinessCondition() {
          @Override
          public String name() {
            return "cond1";
          }

          @Override
          public boolean getAsBoolean() {
            return cond1.get();
          }
        };

    final var c2 =
        new Readiness.ReadinessCondition() {
          @Override
          public String name() {
            return "cond2";
          }

          @Override
          public boolean getAsBoolean() {
            return cond2.get();
          }
        };

    final var hookCalled = new AtomicBoolean(false);
    final var readiness = Readiness.start(List.of(c1, c2), () -> hookCalled.set(true));

    // Set conditions true after some time
    final var t =
        new Thread(
            () -> {
              try {
                Thread.sleep(100);
              } catch (InterruptedException ignored) {
              }
              cond1.set(true);
              cond2.set(true);
            });
    t.start();

    // Wait for readiness to complete
    readiness.future().get(1, TimeUnit.SECONDS);

    assertTrue(cond1.get());
    assertTrue(cond2.get());
    assertTrue(hookCalled.get());
    assertTrue(readiness.future().isDone());

    readiness.close();
  }

  @Test
  void testConditionNeverMetAndClose() throws Exception {
    final var c =
        new Readiness.ReadinessCondition() {
          @Override
          public String name() {
            return "never";
          }

          @Override
          public boolean getAsBoolean() {
            return false;
          }
        };

    final var readiness = Readiness.start(List.of(c), null);

    // Future is not yet done
    assertFalse(readiness.future().isDone());

    // Close immediately
    readiness.close();

    // The future should complete exceptionally when interrupted
    assertTrue(readiness.future().isDone());
  }

  @Test
  void testConditionThrowsException() throws Exception {
    final var c =
        new Readiness.ReadinessCondition() {
          @Override
          public String name() {
            return "error";
          }

          @Override
          public boolean getAsBoolean() {
            throw new RuntimeException("fail");
          }
        };

    final var readiness = Readiness.start(List.of(c), null);

    // Wait a bit to let exception occur
    Thread.sleep(200);

    // Hook should still run
    readiness.close();
    assertTrue(readiness.future().isDone());
    assertThrows(ExecutionException.class, () -> readiness.future().get());
  }

  @Test
  void testTwoConditionsMetOneThrows() throws Exception {
    final var cond1 = new AtomicBoolean(false);
    final var cond2 = new AtomicBoolean(false);
    final var hookCalled = new AtomicBoolean(false);

    final var c1 =
        new Readiness.ReadinessCondition() {
          @Override
          public String name() {
            return "cond1";
          }

          @Override
          public boolean getAsBoolean() {
            return cond1.get();
          }
        };

    final var c2 =
        new Readiness.ReadinessCondition() {
          @Override
          public String name() {
            return "cond2";
          }

          @Override
          public boolean getAsBoolean() {
            return cond2.get();
          }
        };

    final var c3 =
        new Readiness.ReadinessCondition() {
          @Override
          public String name() {
            return "failCond";
          }

          @Override
          public boolean getAsBoolean() {
            throw new RuntimeException("intentional failure");
          }
        };

    final var readiness = Readiness.start(List.of(c1, c2, c3), () -> hookCalled.set(true));

    // Set successful conditions to true after a short delay
    final var t =
        new Thread(
            () -> {
              try {
                Thread.sleep(100);
              } catch (InterruptedException ignored) {
              }
              cond1.set(true);
              cond2.set(true);
            });
    t.start();

    // Wait a bit to let the exception and completions occur
    Thread.sleep(300);

    // Close the readiness (interrupts failing loop)
    readiness.close();

    // The two successful conditions should be true
    assertTrue(cond1.get());
    assertTrue(cond2.get());

    // The termination hook should not run, because one condition threw exception
    assertFalse(hookCalled.get());

    // Future is done after close
    assertTrue(readiness.future().isDone());
  }
}
