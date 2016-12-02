package ru.dorofeev.sandbox.quartzworkflow.execution;

import ru.dorofeev.sandbox.quartzworkflow.utils.Clock;
import ru.dorofeev.sandbox.quartzworkflow.utils.ErrorObservable;
import ru.dorofeev.sandbox.quartzworkflow.utils.Stopwatch;
import ru.dorofeev.sandbox.quartzworkflow.utils.StopwatchFactory;
import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static rx.Observable.interval;

class FixedThreadedExecutorService implements ExecutorService {

	private final PublishSubject<Event> events = PublishSubject.create();
	private final ErrorObservable errors = new ErrorObservable();
	private final Executor executor;
	private final IdleMonitor idleMonitor;
	private final StopwatchFactory stopwatchFactory;
	private final Clock clock;

	private boolean suspended = true;

	FixedThreadedExecutorService(int nThreads, long idleInterval, StopwatchFactory stopwatchFactory, Clock clock) {
		this.executor = Executors.newFixedThreadPool(nThreads);
		this.stopwatchFactory = stopwatchFactory;
		this.clock = clock;

		this.idleMonitor = new IdleMonitor(nThreads, idleInterval);
		this.idleMonitor.idleEvents().subscribe(events);
	}

	@Override
	public rx.Observable<Event> bind(rx.Observable<Cmd> input) {

		input.ofType(ScheduleJobCmd.class)
			.compose(errors.doOnNextRetry(cmd -> { if (suspended) throw new IllegalStateException("Executor service is in suspended state."); } ))
			.compose(errors.doOnNextRetry(cmd -> idleMonitor.threadAcquired()))
			.observeOn(Schedulers.from(executor))
			.compose(errors.mapRetry(this::execute))
			.compose(errors.doOnNextRetry(event -> idleMonitor.threadReleased()))
			.subscribe(events);

		return events;
	}

	private JobCompletedEvent execute(ScheduleJobCmd cmd) {
		Stopwatch stopwatch = stopwatchFactory.newStopwatch();
		try {
			cmd.getExecutable().execute(cmd.getJobId(), cmd.getArgs());
			return new JobCompletedEvent(cmd.getJobId(), null, stopwatch.elapsed(), clock.currentTime());
		} catch (Throwable e) {
			return new JobCompletedEvent(cmd.getJobId(), e, stopwatch.elapsed(), clock.currentTime());
		}
	}

	@Override
	public Observable<Throwable> getErrors() {
		return errors.asObservable();
	}

	@Override
	public void start() {
		suspended = false;
		idleMonitor.release();
	}

	@Override
	public void shutdown() {
		suspended = true;
		idleMonitor.suspend();
	}

	private static class IdleMonitor {

		private final AtomicInteger executingJobs = new AtomicInteger(0);
		private final PublishSubject<IdleEvent> idleEvents = PublishSubject.create();
		private Subscription tickSubscription;

		private final int nThreads;
		private final long idleInterval;

		private boolean suspended = true;
		private final Object suspendedSync = new Object();

		private IdleMonitor(int nThreads, long idleInterval) {
			this.nThreads = nThreads;
			this.idleInterval = idleInterval;

			int load = executingJobs.intValue();
			adjustIdleTicks(load);
		}

		Observable<IdleEvent> idleEvents() {
			return idleEvents;
		}

		private synchronized void adjustIdleTicks(int currentLoad) {
			if (currentLoad < nThreads && tickSubscription == null) {
				tickSubscription = interval(0, idleInterval, MILLISECONDS)
					.subscribe(v -> emitIdleEvent());

			} else if (currentLoad >= nThreads && tickSubscription != null) {
				tickSubscription.unsubscribe();
				tickSubscription = null;
			}
		}

		private void emitIdleEvent() {
			synchronized (suspendedSync) {
				if (suspended)
					return;

				int freeThreadsCount = nThreads - executingJobs.intValue();
				if (freeThreadsCount > 0)
					idleEvents.onNext(new IdleEvent(freeThreadsCount));
			}
		}

		void threadAcquired() {
			int load = executingJobs.incrementAndGet();
			adjustIdleTicks(load);
		}

		void threadReleased() {
			int load = executingJobs.decrementAndGet();
			adjustIdleTicks(load);
		}

		void suspend() {
			synchronized (suspendedSync) {		// ensures that after this block no idle event will be emitted
				this.suspended = true;
			}
		}

		void release() {
			synchronized (suspendedSync) {
				this.suspended = false;
			}
		}
	}
}
