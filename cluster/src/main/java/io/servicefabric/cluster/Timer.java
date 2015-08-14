package io.servicefabric.cluster;

import com.google.common.base.Preconditions;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

class Timer {

	private static final Logger LOGGER = LoggerFactory.getLogger(Timer.class);

	private HashedWheelTimer hashedWheelTimer = new HashedWheelTimer();
	private ConcurrentMap<String, Timeout> timeoutMap = new ConcurrentHashMap<>();

	public void start() {
		hashedWheelTimer.start();
	}

	public void stop() {
		hashedWheelTimer.stop();
		timeoutMap.clear();
	}

	/**
	 * Schedule runnable task to be executed after delay
	 *
	 * @param runnable task which will be executed after delay
	 * @param delay delay to schedule
	 * @param timeUnit time units for delay
	 * @return Timeout object, which can be canceled. {@link io.netty.util.Timeout}
	 */
	public Timeout schedule(final Runnable runnable, int delay, TimeUnit timeUnit) {
		Preconditions.checkNotNull(runnable);
		Preconditions.checkArgument(delay > 0);
		return hashedWheelTimer.newTimeout(new TimerTask() {
			@Override
			public void run(Timeout timeout) throws Exception {
				if (!timeout.isCancelled()) {
					runnable.run();
				}
			}
		}, delay, timeUnit);
	}

	/**
	 * Schedule runnable task to be executed after delay, store timeout internally and map it to id.
	 * If task with {@code id} has been previously scheduled, it would be cancelled and replaced with new one.
	 * Task can be canceled with id used com.playtech.openapi.core.expiration.ITimerService#cancel(java.lang.String) method
	 */
	public void schedule(final String id, final Runnable runnable, int delay, TimeUnit timeUnit) {
		Preconditions.checkArgument(id != null && !id.isEmpty());
		Preconditions.checkNotNull(runnable);
		Preconditions.checkArgument(delay > 0);
		final Timeout oldTask = timeoutMap.remove(id);
		if(oldTask != null) {
			LOGGER.warn("Replacing previously scheduled task for id {} with new one.", id);
			oldTask.cancel();
		}
		Timeout timeout = hashedWheelTimer.newTimeout(new TimerTask() {
			@Override
			public void run(Timeout timeout) throws Exception {
				if (!timeout.isCancelled()) {
					runnable.run();
				}
				timeoutMap.remove(id);
			}
		}, delay, timeUnit);
		timeoutMap.put(id, timeout);
	}

	/**
	 * Cancel timer task with specific id
	 */
	public void cancel(String id) {
		Timeout timeout = timeoutMap.remove(id);
		if (timeout != null) {
			timeout.cancel();
		}
	}
}
