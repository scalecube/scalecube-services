package io.scalecube.services.concurrent;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.atLeast;

import io.scalecube.services.concurrent.ResourceConductor.ResourceHandle;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentTerminationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.verification.Timeout;

@org.junit.jupiter.api.Timeout(value = 5, unit = TimeUnit.SECONDS)
public class ResourceConductorTest {

  private final ResourceConductor conductor = new ResourceConductor(new Properties(), null, null);

  @AfterEach
  void tearDown() {
    conductor.close();
  }

  private static List<String> allThreads() {
    return Thread.getAllStackTraces().keySet().stream().map(Thread::getName).toList();
  }

  @Test
  void shouldLaunchAgentOnDedicatedThread() throws Exception {
    String roleName = "dedicated-agent-test";
    Agent mockAgent = mock(Agent.class);
    when(mockAgent.roleName()).thenReturn(roleName);

    conductor.agent(mockAgent).awaitAdded();

    // Give it up to 1 second to invoke on the background thread
    verify(mockAgent, new Timeout(1000, atLeast(1))).doWork();

    assertTrue(
        allThreads().stream().anyMatch(name -> name.contains(roleName)),
        "Thread for agent should be running");
  }

  @Test
  void shouldAddAgentAndRemoveIt() throws Exception {
    Agent mockAgent = mock(Agent.class);
    when(mockAgent.roleName()).thenReturn("added-agent-test");
    ErrorHandler errorHandler = mock(ErrorHandler.class);
    RuntimeException failure = new RuntimeException("doWork failed");
    when(mockAgent.doWork()).thenThrow(failure).thenReturn(0);

    final var handle = conductor.addAgent(mockAgent, errorHandler);

    verify(errorHandler, new Timeout(1000, atLeast(1))).onError(failure);

    handle.tryRemove();

    verify(mockAgent).onClose();
  }

  @Test
  void shouldGroupAgentsInSharedThread() throws Exception {
    String sharedName = "shared-group";
    Agent agent1 = mock(Agent.class);
    Agent agent2 = mock(Agent.class);
    when(agent1.roleName()).thenReturn("a1");
    when(agent2.roleName()).thenReturn("a2");

    conductor.agent(agent1).invokerName(sharedName).awaitAdded();
    conductor.agent(agent2).invokerName(sharedName).awaitAdded();

    // Give it up to 1 second to invoke on the background thread
    verify(agent1, new Timeout(1000, atLeast(1))).doWork();
    verify(agent2, new Timeout(1000, atLeast(1))).doWork();

    long matchingThreads = allThreads().stream().filter(name -> name.contains(sharedName)).count();

    // Should only be 1 thread for the DynamicCompositeAgent
    assertEquals(
        1, matchingThreads, "Multiple agents in same invoker group should share one thread");
  }

  @Test
  void shouldCloseResourcesInReverseOrder() throws Exception {
    AutoCloseable res1 = mock(AutoCloseable.class);
    AutoCloseable res2 = mock(AutoCloseable.class);

    conductor.addResource(res1);
    conductor.addResource(res2);

    conductor.close();

    var inOrder = Mockito.inOrder(res1, res2);

    // Verify resources were closed in reverse order
    inOrder.verify(res2).close();
    inOrder.verify(res1).close();
  }

  @Test
  void shouldWrapInvokerAndExecuteWork() {
    AtomicInteger workCount = new AtomicInteger(0);
    AgentInvoker mockInvoker = mock(AgentInvoker.class);
    Agent innerAgent = mock(Agent.class);

    when(mockInvoker.agent()).thenReturn(innerAgent);
    when(innerAgent.roleName()).thenReturn("inner-agent");
    when(mockInvoker.invoke()).thenAnswer(inv -> workCount.incrementAndGet());

    AutoCloseable resource = mock(AutoCloseable.class);

    // Add invoker to a shared thread
    conductor.agentInvoker("invoker-group", resource, r -> mockInvoker).awaitAdded();

    // Wait briefly for the agent runner to pick up the work
    long timeout = System.currentTimeMillis() + 2000;
    while (workCount.get() == 0 && System.currentTimeMillis() < timeout) {
      Thread.yield();
    }

    assertTrue(workCount.get() > 0, "Invoker work should have been executed by conductor thread");
  }

  @Test
  void shouldStopThreadsOnClose() throws Exception {
    String roleName = "shutdown-test";
    Agent agent = mock(Agent.class);
    when(agent.roleName()).thenReturn(roleName);

    conductor.agent(agent).awaitAdded();

    // Give it up to 1 second to invoke on the background thread
    verify(agent, new Timeout(1000, atLeast(1))).doWork();

    conductor.close();

    // Give it up to 1 second to close on the background thread
    verify(agent, timeout(1000)).onClose();

    Thread.sleep(1000);

    assertFalse(
        allThreads().stream().anyMatch(name -> name.contains(roleName)),
        "Agent thread should be terminated after conductor close");
  }

  @Test
  void shouldApplyConfigurationFromProperties() {
    Properties props = new Properties();
    props.setProperty("custom-group.idle.strategy", "yield");

    try (ResourceConductor configConductor = new ResourceConductor(props, null, null)) {
      Agent agent = mock(Agent.class);
      when(agent.roleName()).thenReturn("config-agent");

      // Launching in a group triggers getIdleStrategy("custom-group")
      assertDoesNotThrow(
          () -> configConductor.agent(agent).invokerName("custom-group").awaitAdded());
    }
  }

  @Test
  void shouldCloseInvokerWhenConductorCloses() throws Exception {
    Agent innerAgent = mock(Agent.class);
    when(innerAgent.roleName()).thenReturn("invoker-role");
    AgentInvoker mockInvoker = spy(new AgentInvoker(Throwable::printStackTrace, null, innerAgent));

    AutoCloseable resource = mock(AutoCloseable.class);

    conductor.agentInvoker("lifecycle-group", resource, r -> mockInvoker).awaitAdded();

    // Give it up to 1 second to invoke on the background thread
    verify(mockInvoker, new Timeout(1000, atLeast(1))).invoke();

    conductor.addResource(resource);

    conductor.close();

    // Give it up to 1 second to close on the background thread
    verify(mockInvoker, timeout(1000)).close();

    // Verify resource close
    verify(resource).close();
  }

  @Test
  void shouldHandleAgentExceptionsGracefully() throws Exception {
    AtomicInteger errorCaught = new AtomicInteger(0);
    ErrorHandler customHandler = ex -> errorCaught.incrementAndGet();

    Agent crashingAgent =
        new Agent() {
          @Override
          public String roleName() {
            return "crasher";
          }

          @Override
          public int doWork() {
            throw new RuntimeException("Boom");
          }
        };

    conductor.agent(crashingAgent).errorHandler(customHandler).awaitAdded();

    // Wait for the thread to execute the duty cycle
    long timeout = System.currentTimeMillis() + 1000;
    while (errorCaught.get() == 0 && System.currentTimeMillis() < timeout) {
      Thread.yield();
    }

    assertTrue(errorCaught.get() > 0, "Custom error handler should have been triggered");
  }

  @Test
  void shouldSelfEjectOnAgentTerminationException() throws Exception {
    String sharedGroup = "eject-group";
    CountDownLatch survivorLatch = new CountDownLatch(5);
    CountDownLatch crasherClosedLatch = new CountDownLatch(1);

    // Agent 1: Crashes with AgentTerminationException
    Agent crashingAgent =
        spy(
            new Agent() {
              @Override
              public String roleName() {
                return "crasher";
              }

              @Override
              public int doWork() {
                throw new AgentTerminationException();
              }

              @Override
              public void onClose() {
                crasherClosedLatch.countDown();
              }
            });

    // Agent 2: Stays alive
    Agent survivorAgent =
        spy(
            new Agent() {
              @Override
              public String roleName() {
                return "survivor";
              }

              @Override
              public int doWork() {
                survivorLatch.countDown();
                return 1;
              }
            });

    conductor.agent(crashingAgent).invokerName(sharedGroup).awaitAdded();
    conductor.agent(survivorAgent).invokerName(sharedGroup).awaitAdded();

    // Give it up to 1 second to invoke on the background thread
    verify(crashingAgent, new Timeout(1000, atLeast(1))).doWork();
    verify(survivorAgent, new Timeout(1000, atLeast(1))).doWork();

    // Verify crasher is closed and ejected
    assertTrue(
        crasherClosedLatch.await(2, TimeUnit.SECONDS),
        "Crasher should have self-ejected and closed");

    // Verify survivor is still ticking
    assertTrue(
        survivorLatch.await(2, TimeUnit.SECONDS),
        "Survivor should continue working in the same thread");
  }

  @Test
  void shouldHandleMultipleSelfEjectionsInSameGroup() throws Exception {
    String sharedGroup = "multi-eject";
    CountDownLatch closeLatch = new CountDownLatch(2);

    Agent crasher1 =
        new Agent() {
          @Override
          public String roleName() {
            return "c1";
          }

          @Override
          public int doWork() {
            throw new AgentTerminationException();
          }

          @Override
          public void onClose() {
            closeLatch.countDown();
          }
        };

    Agent crasher2 =
        new Agent() {
          @Override
          public String roleName() {
            return "c2";
          }

          @Override
          public int doWork() {
            throw new AgentTerminationException();
          }

          @Override
          public void onClose() {
            closeLatch.countDown();
          }
        };

    conductor.agent(crasher1).invokerName(sharedGroup).awaitAdded();
    conductor.agent(crasher2).invokerName(sharedGroup).awaitAdded();

    // Both should be able to remove themselves via reflection
    assertTrue(closeLatch.await(2, TimeUnit.SECONDS), "Both agents should have ejected themselves");
  }

  @Test
  void shouldStopDedicatedAgentOnTerminationException() throws Exception {
    CountDownLatch onCloseLatch = new CountDownLatch(1);
    Agent terminatingAgent =
        spy(
            new Agent() {
              @Override
              public String roleName() {
                return "dedicated-terminator";
              }

              @Override
              public int doWork() {
                throw new AgentTerminationException();
              }

              @Override
              public void onClose() {
                onCloseLatch.countDown();
              }
            });

    conductor.agent(terminatingAgent).awaitAdded();

    assertTrue(
        onCloseLatch.await(2, TimeUnit.SECONDS),
        "onClose must be called and thread must terminate");

    Thread.sleep(1000);

    assertFalse(
        allThreads().stream().anyMatch(n -> n.contains("dedicated-terminator")),
        "dedicated runner thread must be gone after termination");
  }

  @Test
  void shouldHandleOnStartExceptionsWithErrorHandler() throws Exception {
    AtomicInteger errors = new AtomicInteger(0);
    ErrorHandler handler = ex -> errors.incrementAndGet();

    Agent startCrashing =
        new Agent() {
          @Override
          public String roleName() {
            return "start-crasher";
          }

          @Override
          public void onStart() {
            throw new RuntimeException("boom in onStart");
          }

          @Override
          public int doWork() {
            return 0;
          }
        };

    // Dedicated path
    conductor.agent(startCrashing).errorHandler(handler).awaitAdded();

    // Shared path
    conductor.agent(startCrashing).invokerName("shared-start").errorHandler(handler).awaitAdded();

    long timeout = System.currentTimeMillis() + 2000;
    while (errors.get() < 2 && System.currentTimeMillis() < timeout) {
      Thread.yield();
    }

    assertTrue(errors.get() >= 2, "onStart exceptions must be routed to the supplied ErrorHandler");
  }

  @Test
  void shouldSupportTryRemoveAndAwaitRemovedOnDedicatedRunner() {
    Agent agent = mock(Agent.class);
    when(agent.roleName()).thenReturn("dedicated-remove-test");

    ResourceHandle handle = conductor.agent(agent).awaitAdded();

    assertTrue(handle.tryRemove(), "tryRemove() should succeed");
    assertDoesNotThrow(handle::awaitRemoved, "awaitRemoved() should return immediately");
  }

  @Test
  void shouldCloseDedicatedRunnerWhenRemoved() {
    Agent agent = mock(Agent.class);
    when(agent.roleName()).thenReturn("dedicated-close-test");

    conductor.agent(agent).awaitAdded().awaitRemoved();

    verify(agent).onClose();
  }

  @Test
  void shouldSupportTryRemoveAndAwaitRemovedOnSharedInvoker() {
    String group = "shared-remove-test";
    Agent agent = mock(Agent.class);
    when(agent.roleName()).thenReturn("shared-remove-agent");

    ResourceHandle handle = conductor.agent(agent).invokerName(group).awaitAdded();

    assertTrue(handle.tryRemove(), "tryRemove() should succeed");
    assertDoesNotThrow(handle::awaitRemoved, "awaitRemoved() should complete");
  }

  @Test
  void shouldThrowIllegalStateExceptionWhenTryingToRemoveBeforeConcludingDedicatedRunner() {
    Agent agent = mock(Agent.class);
    ResourceHandle handle = conductor.agent(agent);

    IllegalStateException ex1 = assertThrows(IllegalStateException.class, handle::tryRemove);
    assertEquals("Illegal state", ex1.getMessage());

    IllegalStateException ex2 = assertThrows(IllegalStateException.class, handle::awaitRemoved);
    assertEquals("Illegal state", ex2.getMessage());
  }

  @Test
  void shouldThrowIllegalStateExceptionWhenTryingToRemoveBeforeConcludingSharedInvoker() {
    String group = "remove-before-conclude-shared";
    Agent agent = mock(Agent.class);
    ResourceHandle handle = conductor.agent(agent).invokerName(group);

    IllegalStateException ex1 = assertThrows(IllegalStateException.class, handle::tryRemove);
    assertEquals("Illegal state", ex1.getMessage());

    IllegalStateException ex2 = assertThrows(IllegalStateException.class, handle::awaitRemoved);
    assertEquals("Illegal state", ex2.getMessage());
  }

  @Test
  void shouldThrowWhenConcludingDedicatedRunnerMoreThanOnce() {
    String roleName = "double-conclude-dedicated";
    Agent agent = mock(Agent.class);
    when(agent.roleName()).thenReturn(roleName);

    ResourceHandle handle = conductor.agent(agent);

    // First conclusion succeeds (via awaitAdded)
    handle.awaitAdded();

    // Any subsequent call to an add-method must fail
    assertThrows(ConcurrentModificationException.class, handle::awaitAdded);
    assertThrows(ConcurrentModificationException.class, handle::tryAdd);
  }

  @Test
  void shouldThrowWhenConcludingSharedInvokerMoreThanOnce() {
    String roleName = "double-conclude-shared";
    String group = "double-conclude-group";
    Agent agent = mock(Agent.class);
    when(agent.roleName()).thenReturn(roleName);

    ResourceHandle handle = conductor.agent(agent).invokerName(group);

    // First conclusion succeeds
    handle.awaitAdded();

    // Any subsequent call to an add-method must fail
    assertThrows(ConcurrentModificationException.class, handle::awaitAdded);
    assertThrows(ConcurrentModificationException.class, handle::tryAdd);
  }

  @Test
  void shouldRemoveAgentFromGroupWhenDoWorkReturnsNegative() throws Exception {
    CountDownLatch closed = new CountDownLatch(1);
    AtomicInteger doWorkCalls = new AtomicInteger();

    conductor
        .agent(
            new Agent() {
              @Override
              public String roleName() {
                return "one-shot";
              }

              @Override
              public int doWork() {
                doWorkCalls.incrementAndGet();
                return -1;
              }

              @Override
              public void onClose() {
                closed.countDown();
              }
            })
        .invokerName("negative-group")
        .awaitAdded();

    assertTrue(closed.await(2, TimeUnit.SECONDS), "agent must be closed after returning -1");
    Thread.sleep(100);
    assertEquals(1, doWorkCalls.get(), "agent must not be invoked after returning -1");
  }

  @Test
  void shouldKeepAgentInGroupAfterPlainException() throws Exception {
    AtomicInteger errors = new AtomicInteger();
    AtomicInteger closed = new AtomicInteger();

    conductor
        .agent(
            new Agent() {
              @Override
              public String roleName() {
                return "thrower";
              }

              @Override
              public int doWork() {
                throw new IllegalStateException("boom");
              }

              @Override
              public void onClose() {
                closed.incrementAndGet();
              }
            })
        .invokerName("plain-exception-group")
        .errorHandler(ex -> errors.incrementAndGet())
        .awaitAdded();

    long deadline = System.currentTimeMillis() + 2000;
    while (errors.get() < 2 && System.currentTimeMillis() < deadline) {
      Thread.yield();
    }

    assertTrue(errors.get() >= 2, "agent must keep running after a plain exception");
    assertEquals(0, closed.get(), "agent must not be closed after a plain exception");
  }

  @Test
  void shouldAddAndRemoveFromGroupThreadWithoutBlocking() throws Exception {
    String group = "same-thread-group";
    CountDownLatch childrenStarted = new CountDownLatch(3);
    CountDownLatch childrenClosed = new CountDownLatch(3);

    Agent spawner =
        new Agent() {
          private final List<ResourceHandle> children = new java.util.ArrayList<>();
          private int cycles;

          @Override
          public String roleName() {
            return "spawner";
          }

          @Override
          public int doWork() {
            if (cycles++ == 0) {
              for (int i = 0; i < 3; i++) {
                children.add(
                    conductor
                        .agent(
                            new Agent() {
                              @Override
                              public String roleName() {
                                return "child";
                              }

                              @Override
                              public void onStart() {
                                childrenStarted.countDown();
                              }

                              @Override
                              public int doWork() {
                                return 0;
                              }

                              @Override
                              public void onClose() {
                                childrenClosed.countDown();
                              }
                            })
                        .invokerName(group)
                        .awaitAdded());
              }
            } else if (childrenStarted.getCount() == 0 && !children.isEmpty()) {
              children.forEach(ResourceHandle::awaitRemoved);
              children.clear();
            }
            return 0;
          }
        };

    conductor.agent(spawner).invokerName(group).awaitAdded();

    assertTrue(childrenStarted.await(2, TimeUnit.SECONDS), "children must be added");
    assertTrue(childrenClosed.await(2, TimeUnit.SECONDS), "children must be removed");
  }

  @Test
  void shouldAddConcurrentlyWithoutLosingAgents() throws Exception {
    int threads = 8;
    int perThread = 500;
    CountDownLatch closed = new CountDownLatch(threads * perThread);
    List<Thread> producers = new java.util.ArrayList<>();

    for (int t = 0; t < threads; t++) {
      Thread producer =
          new Thread(
              () -> {
                for (int i = 0; i < perThread; i++) {
                  conductor
                      .agent(
                          new Agent() {
                            @Override
                            public String roleName() {
                              return "short-lived";
                            }

                            @Override
                            public int doWork() {
                              return -1;
                            }

                            @Override
                            public void onClose() {
                              closed.countDown();
                            }
                          })
                      .invokerName("concurrent-group")
                      .tryAdd();
                }
              });
      producers.add(producer);
      producer.start();
    }
    for (Thread producer : producers) {
      producer.join();
    }

    assertTrue(closed.await(4, TimeUnit.SECONDS), "every added agent must run and be closed");
  }

  @Test
  void shouldThrowWhenMixingTryAddAndAwaitAdded() {
    String roleName = "mix-conclude";
    Agent agent = mock(Agent.class);
    when(agent.roleName()).thenReturn(roleName);

    ResourceHandle handle = conductor.agent(agent);

    // First conclusion via tryAdd
    assertTrue(handle.tryAdd(), "first tryAdd must succeed");

    // Subsequent call via awaitAdded must fail
    assertThrows(ConcurrentModificationException.class, handle::awaitAdded);

    // Even another tryAdd after the first one fails
    assertThrows(ConcurrentModificationException.class, handle::tryAdd);
  }

  @Test
  void shouldWaitForOnStartOnDedicatedRunner() {
    AtomicBoolean started = new AtomicBoolean();

    conductor
        .agent(
            new Agent() {
              @Override
              public String roleName() {
                return "slow-start";
              }

              @Override
              public void onStart() {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200));
                started.set(true);
              }

              @Override
              public int doWork() {
                return 0;
              }
            })
        .awaitAdded();

    assertTrue(started.get(), "awaitAdded must return after onStart");
  }

  @Test
  void shouldReturnFromAwaitAddedWhenOnStartFailsOnDedicatedRunner() {
    Agent agent = mock(Agent.class);
    when(agent.roleName()).thenReturn("failing-start");
    doThrow(new IllegalStateException("boom")).when(agent).onStart();

    conductor.agent(agent).errorHandler(ex -> {}).awaitAdded();

    verify(agent).onClose();
  }

  @Test
  void shouldCloseAgentsAddedAfterClose() {
    conductor.close();
    Agent dedicated = mock(Agent.class);
    when(dedicated.roleName()).thenReturn("after-close-dedicated");
    Agent grouped = mock(Agent.class);
    when(grouped.roleName()).thenReturn("after-close-grouped");

    conductor.agent(dedicated).awaitAdded();
    conductor.agent(grouped).invokerName("after-close-group").awaitAdded();

    verify(dedicated).onClose();
    verify(grouped).onClose();
    long deadline = System.currentTimeMillis() + 2000;
    while (isThreadRunning("after-close") && System.currentTimeMillis() < deadline) {
      Thread.yield();
    }
    assertFalse(isThreadRunning("after-close"), "no thread must be left running");
  }

  private static boolean isThreadRunning(String namePart) {
    return allThreads().stream().anyMatch(name -> name.contains(namePart));
  }

  @Test
  void shouldCloseAgentLeftInCompositeAddSlotOnClose() throws Exception {
    String group = "add-slot-group";
    CountDownLatch slotFilled = new CountDownLatch(1);
    CountDownLatch proceed = new CountDownLatch(1);
    CountDownLatch childClosed = new CountDownLatch(1);
    AtomicBoolean childStarted = new AtomicBoolean();

    Agent child =
        new Agent() {
          @Override
          public String roleName() {
            return "add-slot-child";
          }

          @Override
          public void onStart() {
            childStarted.set(true);
          }

          @Override
          public int doWork() {
            return 0;
          }

          @Override
          public void onClose() {
            childClosed.countDown();
          }
        };

    Agent spawner =
        new Agent() {
          private int cycles;

          @Override
          public String roleName() {
            return "add-slot-spawner";
          }

          @Override
          public int doWork() throws Exception {
            switch (cycles++) {
              case 0 -> conductor.agent(child).invokerName(group).tryAdd();
              case 1 -> {
                slotFilled.countDown();
                proceed.await();
              }
              default -> {}
            }
            return 0;
          }
        };

    conductor.agent(spawner).invokerName(group).awaitAdded();
    assertTrue(slotFilled.await(2, TimeUnit.SECONDS), "child must be in the add slot");

    Thread closer = new Thread(conductor::close);
    closer.start();
    Thread.sleep(100);
    proceed.countDown();
    closer.join();

    assertTrue(childClosed.await(2, TimeUnit.SECONDS), "child must be closed");
    assertFalse(childStarted.get(), "child must not have been started");
  }
}
