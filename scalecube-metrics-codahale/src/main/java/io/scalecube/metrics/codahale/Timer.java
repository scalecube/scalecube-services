package io.scalecube.metrics.codahale;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class Timer implements io.scalecube.metrics.api.Timer {
  private final com.codahale.metrics.Timer timer;

  private static class Context implements io.scalecube.metrics.api.Timer.Context {
    private final com.codahale.metrics.Timer.Context context;

    private Context(com.codahale.metrics.Timer.Context context) {
      this.context = context;
    }

    @Override
    public void stop() {
      this.context.stop();
    }
  }

  public Timer(com.codahale.metrics.Timer timer) {
    this.timer = timer;
  }

  @Override
  public void update(long duration, TimeUnit unit) {
    timer.update(duration, unit);
  }

  @Override
  public <T> T time(Callable<T> event) throws Exception {
    return timer.time(event);
  }

  @Override
  public Context time() {
    return new Context(timer.time());
  }
}
