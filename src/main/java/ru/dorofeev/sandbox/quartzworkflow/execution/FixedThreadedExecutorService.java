package ru.dorofeev.sandbox.quartzworkflow.execution;

import ru.dorofeev.sandbox.quartzworkflow.utils.ErrorObservable;
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

	FixedThreadedExecutorService(int nThreads, long idleInterval) {
		this.executor = Executors.newFixedThreadPool(nThreads);

		this.idleMonitor = new IdleMonitor(nThreads, idleInterval);
		this.idleMonitor.idleEvents().subscribe(events);
	}

	@Override
	public rx.Observable<Event> bind(rx.Observable<Cmd> input) {

		input.ofType(ScheduleTaskCmd.class)
			.compose(errors.doOnNextRetry(cmd -> idleMonitor.threadAcquired()))
			.observeOn(Schedulers.from(executor))
			.compose(errors.mapRetry(cmd -> {
					try {
						cmd.getExecutable().execute(cmd.getTaskId(), cmd.getArgs());
						return new TaskCompletedEvent(cmd.getTaskId(), null);
					} catch (Throwable e) {
						return new TaskCompletedEvent(cmd.getTaskId(), e);
					}
				}))
			.compose(errors.doOnNextRetry(event -> idleMonitor.threadReleased()))
			.subscribe(events);

		return events;
	}

	@Override
	public Observable<Throwable> getErrors() {
		return errors.asObservable();
	}

	private static class IdleMonitor {

		private final AtomicInteger executingTasks = new AtomicInteger(0);
		private final PublishSubject<IdleEvent> idleEvents = PublishSubject.create();
		private Subscription tickSubscription;

		private final int nThreads;
		private final long idleInterval;

		private IdleMonitor(int nThreads, long idleInterval) {
			this.nThreads = nThreads;
			this.idleInterval = idleInterval;

			int load = executingTasks.intValue();
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
			int freeThreadsCount = nThreads - executingTasks.intValue();
			if (freeThreadsCount > 0)
				idleEvents.onNext(new IdleEvent(freeThreadsCount));
		}

		void threadAcquired() {
			int load = executingTasks.incrementAndGet();
			adjustIdleTicks(load);
		}

		void threadReleased() {
			int load = executingTasks.decrementAndGet();
			adjustIdleTicks(load);
		}
	}
}
