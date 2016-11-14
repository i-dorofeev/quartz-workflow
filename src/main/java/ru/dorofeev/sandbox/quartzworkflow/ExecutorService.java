package ru.dorofeev.sandbox.quartzworkflow;

import rx.Subscription;
import rx.schedulers.Schedulers;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static rx.Observable.interval;

public class ExecutorService {

	public static ScheduleTaskCmd scheduleTaskCmd(TaskId task, Executable runnable) {
		return new ScheduleTaskCmd(task, runnable);
	}

	public static TaskCompletedEvent taskSuccessfullyCompletedEvent(TaskId taskId) {
		return new TaskCompletedEvent(taskId, null);
	}

	public static TaskCompletedEvent taskFailedEvent(TaskId taskId, Throwable e) {
		return new TaskCompletedEvent(taskId, e);
	}

	public interface Cmd { }

	@SuppressWarnings("WeakerAccess")	// must be public since it is exposed by public static method
	public static class ScheduleTaskCmd implements Cmd {

		private final TaskId taskId;
		private final Executable executable;

		ScheduleTaskCmd(TaskId taskId, Executable executable) {
			this.taskId = taskId;
			this.executable = executable;
		}

		public TaskId getTaskId() {
			return taskId;
		}

		Executable getExecutable() {
			return executable;
		}
	}

	public interface Event { }

	public static class TaskCompletedEvent implements Event {

		private final TaskId taskId;
		private final Throwable exception;

		TaskCompletedEvent(TaskId taskId, Throwable exception) {
			this.taskId = taskId;
			this.exception = exception;
		}

		public TaskId getTaskId() {
			return taskId;
		}

		public Throwable getException() {
			return exception;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TaskCompletedEvent that = (TaskCompletedEvent) o;

			return taskId.equals(that.taskId) && (exception != null ? exception.equals(that.exception) : that.exception == null);
		}

		@Override
		public int hashCode() {
			int result = taskId.hashCode();
			result = 31 * result + (exception != null ? exception.hashCode() : 0);
			return result;
		}

		@Override
		public String toString() {
			return "TaskCompletedEvent{" +
				"taskId=" + taskId +
				", exception=" + exception +
				'}';
		}
	}

	public static class IdleEvent implements Event {

		private final int freeThreadsCount;

		IdleEvent(int freeThreadsCount) {
			this.freeThreadsCount = freeThreadsCount;
		}

		public int getFreeThreadsCount() {
			return freeThreadsCount;
		}

		@Override
		public String toString() {
			return "IdleEvent{" +
				"freeThreadsCount=" + freeThreadsCount +
				'}';
		}
	}

	private final ObservableHolder<Event> events = new ObservableHolder<>();
	private final Executor executor;
	private final IdleMonitor idleMonitor;

	public ExecutorService(int nThreads, long idleInterval) {
		this.executor = Executors.newFixedThreadPool(nThreads);

		this.idleMonitor = new IdleMonitor(nThreads, idleInterval);
		this.idleMonitor.idleEvents().getObservable().subscribe(events.nextObserver());
	}

	public rx.Observable<Event> bind(rx.Observable<Cmd> input) {

		input.ofType(ScheduleTaskCmd.class)
			.doOnNext(cmd -> idleMonitor.threadAcquired())
			.observeOn(Schedulers.from(executor))
			.map(cmd -> {
					try {
						cmd.getExecutable().execute();
						return new TaskCompletedEvent(cmd.getTaskId(), null);
					} catch (Throwable e) {
						return new TaskCompletedEvent(cmd.getTaskId(), e);
					}
				})
			.doOnNext(event -> idleMonitor.threadReleased())
			.subscribe(events.nextObserver());

		return events.getObservable();
	}

	private static class IdleMonitor {

		private final AtomicInteger executingTasks = new AtomicInteger(0);
		private final ObservableHolder<IdleEvent> idleEvents = new ObservableHolder<>();
		private Subscription tickSubscription;

		private final int nThreads;
		private final long idleInterval;

		private IdleMonitor(int nThreads, long idleInterval) {
			this.nThreads = nThreads;
			this.idleInterval = idleInterval;

			int load = executingTasks.intValue();
			adjustIdleTicks(load);
		}

		ObservableHolder<IdleEvent> idleEvents() {
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
