package ru.dorofeev.sandbox.quartzworkflow.execution;

import ru.dorofeev.sandbox.quartzworkflow.JobDataMap;
import ru.dorofeev.sandbox.quartzworkflow.TaskId;
import rx.Observable;

public interface ExecutorService {

	static ScheduleTaskCmd scheduleTaskCmd(TaskId task, JobDataMap args, Executable runnable) {
		return new ScheduleTaskCmd(task, args, runnable);
	}

	static TaskCompletedEvent taskSuccessfullyCompletedEvent(TaskId taskId) {
		return new TaskCompletedEvent(taskId, null);
	}

	static TaskCompletedEvent taskFailedEvent(TaskId taskId, Throwable e) {
		return new TaskCompletedEvent(taskId, e);
	}

	rx.Observable<Event> bind(rx.Observable<Cmd> input);

	Observable<Throwable> getErrors();

	interface Cmd { }

	interface Event { }

	class ScheduleTaskCmd implements Cmd {

		private final TaskId taskId;
		private final JobDataMap args;
		private final Executable executable;

		ScheduleTaskCmd(TaskId taskId, JobDataMap args, Executable executable) {
			this.taskId = taskId;
			this.args = args;
			this.executable = executable;
		}

		public TaskId getTaskId() {
			return taskId;
		}

		Executable getExecutable() {
			return executable;
		}

		JobDataMap getArgs() {
			return args;
		}
	}

	class TaskCompletedEvent implements Event {

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

			ExecutorService.TaskCompletedEvent that = (TaskCompletedEvent) o;

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

	class IdleEvent implements Event {

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
}
