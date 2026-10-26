package io.scalecube.services.concurrent;

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.DynamicCompositeAgent;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Execution and resource manager for {@link Agent}, {@link AgentInvoker}, {@link AutoCloseable}
 * objects.
 *
 * <p>Responsible for: orchestrating multiple agents/invokers into shared threads or assigning them
 * to dedicated runners. Managing {@link AutoCloseable} resources to ensure clean shut down in
 * proper order. Simplifying agent configuration to configure idle strategies, thread factories, and
 * error handling before launching agents.
 *
 * <p>Shared thread groups (see {@link AgentBootstrap#invokerName(String)}) run on an Agrona {@link
 * DynamicCompositeAgent}. An agent leaves its group when its {@link Agent#doWork()} returns a
 * negative value or throws {@link AgentTerminationException}, or when its handle is removed. A
 * plain exception does not remove the agent: it is reported to the error handler and {@link
 * Agent#doWork()} is called again on the next duty cycle, as with {@link AgentRunner}. An agent
 * that cannot recover must throw {@link AgentTerminationException} or return a negative value. The
 * default error handler logs at most one exception per second per agent and counts the suppressed
 * ones. Adding and removing agents never blocks the group thread, so agents of a group may add and
 * remove other agents of the same group.
 *
 * <p>An agent without a group runs on its own {@link AgentRunner}; removing its handle closes that
 * runner. {@link ResourceHandle#awaitAdded()} and {@link ResourceHandle#awaitRemoved()} wait at
 * most {@link #AWAIT_TIMEOUT_MS} and then throw {@link IllegalStateException}.
 *
 * <p>Closing the conductor closes all groups and runners, then the tracked resources in reverse
 * order of registration. An agent added after that is closed right away.
 */
public class ResourceConductor implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ResourceConductor.class);

  public static final long AWAIT_TIMEOUT_MS = 10_000;

  private final Properties properties;
  private final ThreadFactorySource threadFactorySource;
  private final ErrorHandler invokerErrorHandler;

  private final ConcurrentMap<String, AgentGroup> groups = new ConcurrentHashMap<>();
  private final List<AgentRunner> agentRunners = new CopyOnWriteArrayList<>();
  private final List<AutoCloseable> resources = new CopyOnWriteArrayList<>();

  private volatile boolean isClosed;

  public ResourceConductor() {
    this(null, null, null);
  }

  public ResourceConductor(
      Properties properties,
      ThreadFactorySource threadFactorySource,
      ErrorHandler invokerErrorHandler) {
    this.properties = properties;
    this.threadFactorySource = threadFactorySource;
    this.invokerErrorHandler = invokerErrorHandler;
  }

  /**
   * Register an already-started {@link AgentInvoker} into named shared thread group.
   *
   * @param invokerName name of the invoker group
   * @param resource lifecycle-managed resource (e.g. MediaDriver, Archive)
   * @param factory factory method to extract {@link AgentInvoker} from the resource
   * @return {@link ResourceHandle} instance
   */
  public <T extends AutoCloseable> ResourceHandle agentInvoker(
      String invokerName, T resource, Function<T, AgentInvoker> factory) {
    return new AgentInvokerHandle(computeGroup(invokerName), factory.apply(resource));
  }

  /**
   * Track resource for closing on conductor shutdown (in reverse registration order).
   *
   * @return original resource for further use
   */
  public <T extends AutoCloseable> T addResource(T resource) {
    resources.add(resource);
    return resource;
  }

  /**
   * Create fluent builder to configure and launch {@link Agent}.
   *
   * @return bootstrap instance
   */
  public AgentBootstrap agent(Agent agent) {
    return new AgentBootstrap(this, agent);
  }

  /**
   * Adds {@link Agent} on its own runner without waiting for it to start.
   *
   * @param agent agent
   * @param errorHandler error handler of the agent
   * @return handle of the agent
   */
  public ResourceHandle addAgent(Agent agent, ErrorHandler errorHandler) {
    final var bootstrap = agent(agent).errorHandler(errorHandler);
    bootstrap.tryAdd();
    return bootstrap;
  }

  private AgentGroup computeGroup(String invokerName) {
    return groups.computeIfAbsent(
        invokerName,
        name -> {
          final var group = new AgentGroup(name);
          group.runner =
              startAgentRunner(
                  group.composite,
                  invokerIdleStrategy(invokerName),
                  invokerThreadFactory(invokerName),
                  invokerErrorHandler(invokerName));
          register(group.runner);
          return group;
        });
  }

  private void register(AgentRunner agentRunner) {
    agentRunners.add(agentRunner);
    if (isClosed) {
      agentRunners.remove(agentRunner);
      CloseHelper.quietClose(agentRunner);
    }
  }

  private static AgentRunner startAgentRunner(
      Agent agent,
      IdleStrategy idleStrategy,
      ThreadFactory threadFactory,
      ErrorHandler errorHandler) {
    final var agentRunner = new AgentRunner(idleStrategy, errorHandler, null, agent);
    if (threadFactory != null) {
      AgentRunner.startOnThread(agentRunner, threadFactory);
    } else {
      AgentRunner.startOnThread(agentRunner);
    }
    return agentRunner;
  }

  private ThreadFactory invokerThreadFactory(String invokerName) {
    return threadFactorySource != null && properties != null
        ? threadFactorySource.threadFactory(properties.getProperty(invokerName + ".thread.factory"))
        : null;
  }

  private IdleStrategy invokerIdleStrategy(String invokerName) {
    return properties != null
        ? idleStrategy(
            properties.getProperty(invokerName + ".idle.strategy", BackoffIdleStrategy.ALIAS))
        : new BackoffIdleStrategy();
  }

  /**
   * Returns the idle strategy of a name: an Agrona alias ({@code noop}, {@code spin}, {@code
   * yield}, {@code sleep-ns}, {@code sleep-ms}, {@code backoff}) or the class name of an {@link
   * IdleStrategy} with a no-arg constructor.
   *
   * @param name alias or class name
   * @return idle strategy
   */
  public static IdleStrategy idleStrategy(String name) {
    return switch (name) {
      case NoOpIdleStrategy.ALIAS -> NoOpIdleStrategy.INSTANCE;
      case BusySpinIdleStrategy.ALIAS -> BusySpinIdleStrategy.INSTANCE;
      case YieldingIdleStrategy.ALIAS -> YieldingIdleStrategy.INSTANCE;
      case SleepingIdleStrategy.ALIAS -> new SleepingIdleStrategy();
      case SleepingMillisIdleStrategy.ALIAS -> new SleepingMillisIdleStrategy();
      case BackoffIdleStrategy.ALIAS -> new BackoffIdleStrategy();
      default -> {
        try {
          yield (IdleStrategy) Class.forName(name).getConstructor().newInstance();
        } catch (Exception ex) {
          throw new IllegalArgumentException("Unknown idle strategy: " + name, ex);
        }
      }
    };
  }

  private ErrorHandler invokerErrorHandler(String invokerName) {
    return invokerErrorHandler != null ? invokerErrorHandler : new LoggingErrorHandler(invokerName);
  }

  private static void await(BooleanSupplier condition, Agent agent, String action) {
    final var idleStrategy = new BackoffIdleStrategy();
    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(AWAIT_TIMEOUT_MS);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() - deadline > 0) {
        throw new IllegalStateException(
            "[" + agent.roleName() + "] " + action + " timed out after " + AWAIT_TIMEOUT_MS + "ms");
      }
      idleStrategy.idle();
    }
  }

  @Override
  public void close() {
    isClosed = true;
    CloseHelper.quietCloseAll(agentRunners);
    final var reversed = new ArrayList<>(resources);
    Collections.reverse(reversed);
    CloseHelper.quietCloseAll(reversed);
  }

  /**
   * Fluent builder for launching an {@link Agent} with custom execution settings: {@link
   * IdleStrategy}, {@link ThreadFactory}, {@link ErrorHandler}.
   */
  public static class AgentBootstrap implements ResourceHandle {

    private static final AtomicIntegerFieldUpdater<AgentBootstrap> IS_CONCLUDED_UPDATER =
        newUpdater(AgentBootstrap.class, "isConcluded");
    private volatile int isConcluded;

    private final ResourceConductor conductor;
    private final Agent agent;
    private String invokerName;
    private IdleStrategy idleStrategy;
    private ThreadFactory threadFactory;
    private ErrorHandler errorHandler;
    private ResourceHandle resourceHandle;

    private AgentBootstrap(ResourceConductor conductor, Agent agent) {
      this.conductor = conductor;
      this.agent = agent;
    }

    public AgentBootstrap invokerName(String invokerName) {
      this.invokerName = invokerName;
      return this;
    }

    public AgentBootstrap idleStrategy(IdleStrategy idleStrategy) {
      this.idleStrategy = idleStrategy;
      return this;
    }

    public AgentBootstrap idleStrategy(String idleStrategy) {
      return idleStrategy(ResourceConductor.idleStrategy(idleStrategy));
    }

    public AgentBootstrap threadFactory(ThreadFactory threadFactory) {
      this.threadFactory = threadFactory;
      return this;
    }

    public AgentBootstrap threadFactory(String threadFactory) {
      final var source = conductor.threadFactorySource;
      return threadFactory(source != null ? source.threadFactory(threadFactory) : null);
    }

    public AgentBootstrap errorHandler(ErrorHandler errorHandler) {
      this.errorHandler = errorHandler;
      return this;
    }

    private void conclude() {
      if (IS_CONCLUDED_UPDATER.getAndSet(this, 1) != 0) {
        throw new ConcurrentModificationException("Already concluded");
      }

      if (errorHandler == null) {
        errorHandler = new LoggingErrorHandler(agent.roleName());
      }

      if (idleStrategy == null) {
        idleStrategy = new BackoffIdleStrategy();
      }

      if (invokerName != null) {
        resourceHandle =
            new AgentInvokerHandle(conductor.computeGroup(invokerName), agent, errorHandler);
      } else {
        final var handle = new AgentRunnerHandle(conductor, agent);
        handle.agentRunner = startAgentRunner(handle, idleStrategy, threadFactory, errorHandler);
        resourceHandle = handle;
      }
    }

    @Override
    public boolean tryAdd() {
      conclude();
      return resourceHandle.tryAdd();
    }

    @Override
    public ResourceHandle awaitAdded() {
      conclude();
      resourceHandle.awaitAdded();
      return this;
    }

    @Override
    public boolean tryRemove() {
      if (resourceHandle == null) {
        throw new IllegalStateException("Illegal state");
      }
      return resourceHandle.tryRemove();
    }

    @Override
    public ResourceHandle awaitRemoved() {
      if (resourceHandle == null) {
        throw new IllegalStateException("Illegal state");
      }
      resourceHandle.awaitRemoved();
      return this;
    }
  }

  /**
   * Named shared thread group: an Agrona {@link DynamicCompositeAgent} plus this agent, which moves
   * pending handles into the composite's single add slot one per duty cycle, so that adding never
   * blocks the caller, including callers on the group thread itself.
   */
  private static class AgentGroup implements Agent {

    private final Queue<AgentInvokerHandle> pendingQueue = new ManyToOneConcurrentLinkedQueue<>();
    private final DynamicCompositeAgent composite;
    private volatile AgentRunner runner;
    private AgentInvokerHandle addedHandle;

    private AgentGroup(String name) {
      composite = new DynamicCompositeAgent(name, this);
    }

    private void add(AgentInvokerHandle handle) {
      pendingQueue.add(handle);
      if (composite.status() == DynamicCompositeAgent.Status.CLOSED) {
        closePendingQueue();
      }
    }

    private boolean isOnGroupThread() {
      final var agentRunner = runner;
      return agentRunner != null && agentRunner.thread() == Thread.currentThread();
    }

    @Override
    public int doWork() {
      final var handle = pendingQueue.peek();
      if (handle != null && composite.tryAdd(handle)) {
        pendingQueue.poll();
        addedHandle = handle;
        return 1;
      }
      return 0;
    }

    @Override
    public void onClose() {
      final var handle = addedHandle;
      addedHandle = null;
      if (handle != null) {
        handle.onClose();
      }
      closePendingQueue();
    }

    private synchronized void closePendingQueue() {
      for (var handle = pendingQueue.poll(); handle != null; handle = pendingQueue.poll()) {
        handle.onClose();
      }
    }

    @Override
    public String roleName() {
      return composite.roleName();
    }
  }

  /**
   * Handle to an agent of an {@link AgentGroup}: runs the agent through an Agrona {@link
   * AgentInvoker} and removes itself from the composite once the agent is done (negative work
   * count), terminated or asked to leave.
   */
  private static class AgentInvokerHandle implements Agent, ResourceHandle {

    private final AgentGroup group;
    private final AgentInvoker agentInvoker;
    private final boolean shouldStart;

    private volatile boolean isStarted;
    private volatile boolean isClosed;
    private volatile boolean isRemoveRequested;

    private AgentInvokerHandle(AgentGroup group, Agent agent, ErrorHandler errorHandler) {
      this.group = group;
      this.agentInvoker = new AgentInvoker(errorHandler, null, agent);
      this.shouldStart = true;
    }

    private AgentInvokerHandle(AgentGroup group, AgentInvoker agentInvoker) {
      this.group = group;
      this.agentInvoker = agentInvoker;
      this.shouldStart = false;
    }

    @Override
    public String roleName() {
      return agentInvoker.agent().roleName();
    }

    @Override
    public void onStart() {
      group.addedHandle = null;
      if (shouldStart) {
        agentInvoker.start();
      }
      isStarted = true;
    }

    @Override
    public int doWork() {
      if (!isRemoveRequested) {
        final int workCount = agentInvoker.invoke();
        if (workCount >= 0 && agentInvoker.isRunning()) {
          return workCount;
        }
        isRemoveRequested = true; // agent is done: leave the group as on an external remove
      }
      // retried every duty cycle until the composite's single remove slot takes it
      group.composite.tryRemove(this);
      return 1;
    }

    @Override
    public void onClose() {
      try {
        agentInvoker.close();
      } finally {
        isClosed = true;
      }
    }

    @Override
    public boolean tryAdd() {
      group.add(this);
      return true;
    }

    @Override
    public ResourceHandle awaitAdded() {
      group.add(this);
      try {
        await(() -> isStarted || isClosed, "awaitAdded");
      } catch (IllegalStateException ex) {
        isRemoveRequested = true;
        throw ex;
      }
      return this;
    }

    @Override
    public boolean tryRemove() {
      isRemoveRequested = true;
      return true;
    }

    @Override
    public ResourceHandle awaitRemoved() {
      isRemoveRequested = true;
      await(() -> isClosed, "awaitRemoved");
      return this;
    }

    private void await(BooleanSupplier condition, String action) {
      if (group.isOnGroupThread()) {
        return; // the group thread itself completes the operation on its next duty cycle
      }
      ResourceConductor.await(condition, agentInvoker.agent(), action);
    }
  }

  private static class LoggingErrorHandler implements ErrorHandler {

    private static final long LOG_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);

    private final String name;
    private long nextLogTimeNs = System.nanoTime();
    private long suppressedCount;

    public LoggingErrorHandler(String name) {
      this.name = name;
    }

    @Override
    public void onError(Throwable ex) {
      final long now = System.nanoTime();
      if (now - nextLogTimeNs < 0) {
        suppressedCount++;
        return;
      }
      if (suppressedCount > 0) {
        LOGGER.error(
            "[{}] Exception occurred ({} more suppressed since the previous one logged): ",
            name,
            suppressedCount,
            ex);
        suppressedCount = 0;
      } else {
        LOGGER.error("[{}] Exception occurred: ", name, ex);
      }
      nextLogTimeNs = now + LOG_INTERVAL_NS;
    }
  }

  /**
   * Handle to an execution resource, e.g. {@link Agent}, {@link AgentInvoker}, {@link AgentRunner}.
   *
   * <p>Provides control over when the execution resource becomes active and when it is removed from
   * the execution context. Responsible for: waiting for safe provisioning, coordinating shutdown
   * and resource cleanup.
   */
  public interface ResourceHandle {

    /**
     * Try to add execution resource to its execution group immediately.
     *
     * @return {@code true} if successfully added, {@code false} otherwise
     */
    boolean tryAdd();

    /**
     * Block until execution resource is successfully added to its execution group (dedicated runner
     * or shared composite), i.e. until its {@link Agent#onStart()} has completed. Called on the
     * group thread itself, it does not block and the resource is added on the next duty cycle.
     * Waits at most {@link ResourceConductor#AWAIT_TIMEOUT_MS}.
     *
     * @return this instance
     * @throws IllegalStateException on timeout
     */
    ResourceHandle awaitAdded();

    /**
     * Try to remove execution resource from its execution group immediately.
     *
     * @return {@code true} if successfully removed, {@code false} otherwise
     */
    boolean tryRemove();

    /**
     * Block until execution resource is removed from its execution group (dedicated runner or
     * shared composite), i.e. until its {@link Agent#onClose()} has completed. Called on the group
     * thread itself, it does not block and the resource is removed on the next duty cycle. Waits at
     * most {@link ResourceConductor#AWAIT_TIMEOUT_MS}.
     *
     * @return this instance
     * @throws IllegalStateException on timeout
     */
    ResourceHandle awaitRemoved();
  }

  private static class AgentRunnerHandle implements Agent, ResourceHandle {

    private final ResourceConductor conductor;
    private final Agent agent;
    private volatile AgentRunner agentRunner;

    private volatile boolean isStarted;
    private volatile boolean isClosed;

    private AgentRunnerHandle(ResourceConductor conductor, Agent agent) {
      this.conductor = conductor;
      this.agent = agent;
    }

    @Override
    public String roleName() {
      return agent.roleName();
    }

    @Override
    public void onStart() {
      agent.onStart();
      isStarted = true;
    }

    @Override
    public int doWork() throws Exception {
      return agent.doWork();
    }

    @Override
    public void onClose() {
      try {
        agent.onClose();
      } finally {
        isClosed = true;
      }
    }

    @Override
    public boolean tryAdd() {
      conductor.register(agentRunner);
      return true;
    }

    @Override
    public ResourceHandle awaitAdded() {
      conductor.register(agentRunner);
      await(() -> isStarted || isClosed, agent, "awaitAdded");
      return this;
    }

    @Override
    public boolean tryRemove() {
      awaitRemoved();
      return true;
    }

    @Override
    public ResourceHandle awaitRemoved() {
      if (agentRunner.thread() == Thread.currentThread()) {
        throw new IllegalStateException(
            "Agent on a dedicated runner cannot remove itself, throw AgentTerminationException");
      }
      conductor.agentRunners.remove(agentRunner);
      CloseHelper.close(agentRunner);
      return this;
    }
  }
}
