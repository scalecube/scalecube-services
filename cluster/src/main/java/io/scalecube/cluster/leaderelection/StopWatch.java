package io.scalecube.cluster.leaderelection;

import java.util.Random;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by ronenn on 9/12/2016.
 */
public abstract class StopWatch {

    private final ScheduledThreadPoolExecutor executor;
    private final int timeout;
    private final TimeUnit timeUnit;
    private final Random rnd;

    public StopWatch(int timeout, TimeUnit timeUnit) {
        executor = new ScheduledThreadPoolExecutor(1);
        this.timeout = timeout;
        this.timeUnit = timeUnit;
        rnd = new Random();
    }

    public void reset() {

        executor.getQueue().clear();
        executor.schedule(new Runnable() {
            @Override
            public void run() {
                onTimeout();
            }
        }, randomTimeOut(timeout / 2, timeout), timeUnit);
    }

    public abstract void onTimeout();

    private int randomTimeOut(int low, int high) {
        return rnd.nextInt(high - low) + low;
    }

}
