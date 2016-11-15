package ru.dorofeev.sandbox.quartzworkflow.execution;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObject;
import rx.Observable;

public interface ExecutorService {

	static ScheduleTaskCmd scheduleTaskCmd(JobId task, SerializedObject args, Executable runnable) {
		return new ScheduleTaskCmd(task, args, runnable);
	}

	static TaskCompletedEvent taskSuccessfullyCompletedEvent(JobId jobId) {
		return new TaskCompletedEvent(jobId, null);
	}

	static TaskCompletedEvent taskFailedEvent(JobId jobId, Throwable e) {
		return new TaskCompletedEvent(jobId, e);
	}

	rx.Observable<Event> bind(rx.Observable<Cmd> input);

	Observable<Throwable> getErrors();

	interface Cmd { }

	interface Event { }

	class ScheduleTaskCmd implements Cmd {

		private final JobId jobId;
		private final SerializedObject args;
		private final Executable executable;

		ScheduleTaskCmd(JobId jobId, SerializedObject args, Executable executable) {
			this.jobId = jobId;
			this.args = args;
			this.executable = executable;
		}

		public JobId getJobId() {
			return jobId;
		}

		Executable getExecutable() {
			return executable;
		}

		SerializedObject getArgs() {
			return args;
		}
	}

	class TaskCompletedEvent implements Event {

		private final JobId jobId;
		private final Throwable exception;

		TaskCompletedEvent(JobId jobId, Throwable exception) {
			this.jobId = jobId;
			this.exception = exception;
		}

		public JobId getJobId() {
			return jobId;
		}

		public Throwable getException() {
			return exception;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			ExecutorService.TaskCompletedEvent that = (TaskCompletedEvent) o;

			return jobId.equals(that.jobId) && (exception != null ? exception.equals(that.exception) : that.exception == null);
		}

		@Override
		public int hashCode() {
			int result = jobId.hashCode();
			result = 31 * result + (exception != null ? exception.hashCode() : 0);
			return result;
		}

		@Override
		public String toString() {
			return "TaskCompletedEvent{" +
				"jobId=" + jobId +
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
