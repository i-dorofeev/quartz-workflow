package ru.dorofeev.sandbox.quartzworkflow;

import rx.Subscription;
import rx.schedulers.Schedulers;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
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

	public static class ScheduleTaskCmd implements Cmd {

		private final TaskId taskId;
		private final Executable executable;

		public ScheduleTaskCmd(TaskId taskId, Executable executable) {
			this.taskId = taskId;
			this.executable = executable;
		}

		public TaskId getTaskId() {
			return taskId;
		}

		public Executable getExecutable() {
			return executable;
		}
	}

	public interface Event { }

	public static class TaskCompletedEvent implements Event {

		private final TaskId taskId;
		private final Throwable exception;

		public TaskCompletedEvent(TaskId taskId, Throwable exception) {
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
	}

	public static class IdleEvent implements Event {

	}

	private final ObservableHolder<Event> events = new ObservableHolder<>();
	private final Executor executor;
	private final IdleMonitor idleMonitor;

	public ExecutorService(int nThreads) {
		this.executor = Executors.newFixedThreadPool(nThreads);

		this.idleMonitor = new IdleMonitor(nThreads);
		this.idleMonitor.idleEvents().getObservable().subscribe(events);
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
			.subscribe(events);

		return events.getObservable();
	}

	private static class IdleMonitor {

		private final AtomicInteger executingTasks = new AtomicInteger(0);
		private final ObservableHolder<IdleEvent> idleEvents = new ObservableHolder<>();
		private Subscription tickSubscription;

		private final int nThreads;

		private IdleMonitor(int nThreads) {
			this.nThreads = nThreads;

			int load = executingTasks.intValue();
			adjustIdleTicks(load);
		}

		ObservableHolder<IdleEvent> idleEvents() {
			return idleEvents;
		}

		private void adjustIdleTicks(int currentLoad) {
			if (currentLoad < nThreads)
				tickSubscription = interval(0, 1, SECONDS).subscribe(v -> idleEvents.onNext(new IdleEvent()));
			else if (!tickSubscription.isUnsubscribed())
				tickSubscription.unsubscribe();
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
