package ru.dorofeev.sandbox.quartzworkflow;

import rx.schedulers.Schedulers;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class ExecutorService {

	public static ScheduleTaskCmd scheduleTaskCmd(TaskId task, Runnable runnable) {
		return new ScheduleTaskCmd(task, runnable);
	}

	public static TaskCompletedEvent taskSuccessfullyCompletedEvent(TaskId taskId) {
		return new TaskCompletedEvent(taskId, null);
	}

	public interface Cmd { }

	public static class ScheduleTaskCmd implements Cmd {

		private final TaskId taskId;
		private final Runnable runnable;

		public ScheduleTaskCmd(TaskId taskId, Runnable runnable) {
			this.taskId = taskId;
			this.runnable = runnable;
		}

		public TaskId getTaskId() {
			return taskId;
		}

		public Runnable getRunnable() {
			return runnable;
		}
	}

	public interface Event { }

	public static class TaskCompletedEvent implements Event {

		private final TaskId taskId;
		private final Exception exception;

		public TaskCompletedEvent(TaskId taskId, Exception exception) {
			this.taskId = taskId;
			this.exception = exception;
		}

		public TaskId getTaskId() {
			return taskId;
		}

		public Exception getException() {
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

	private final ObservableHolder<Event> events = new ObservableHolder<>();
	//private final ObservableHolder<Exception> errors = new ObservableHolder<>();

	private final Executor executor;

	public ExecutorService(int nThreads) {
		this.executor = Executors.newFixedThreadPool(nThreads);
	}

	public rx.Observable<Event> bind(rx.Observable<Cmd> input) {

		input.ofType(ScheduleTaskCmd.class)
			.observeOn(Schedulers.from(executor))
			.subscribe(scheduleTaskCmd -> {
				try {
					scheduleTaskCmd.getRunnable().run();
					events.onNext(new TaskCompletedEvent(scheduleTaskCmd.getTaskId(), null));
				} catch (RuntimeException e) {
					events.onNext(new TaskCompletedEvent(scheduleTaskCmd.getTaskId(), e));
				}
			});

		return events.getObservable();
	}
}
