package ru.dorofeev.sandbox.quartzworkflow.execution;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObject;
import rx.Observable;

import java.util.Date;

public interface ExecutorService {

	static ScheduleJobCmd scheduleJobCmd(JobId jobId, SerializedObject args, Executable runnable) {
		return new ScheduleJobCmd(jobId, args, runnable);
	}

	static JobCompletedEvent jobSuccessfullyCompletedEvent(JobId jobId, long executionDuration, Date completed) {
		return new JobCompletedEvent(jobId, null, executionDuration, completed);
	}

	static JobCompletedEvent jobFailedEvent(JobId jobId, Throwable e, long executionDuration, Date completed) {
		return new JobCompletedEvent(jobId, e, executionDuration, completed);
	}

	rx.Observable<Event> bind(rx.Observable<Cmd> input);

	Observable<Throwable> getErrors();

	void start();

	void shutdown();

	interface Cmd { }

	interface Event { }

	class ScheduleJobCmd implements Cmd {

		private final JobId jobId;
		private final SerializedObject args;
		private final Executable executable;

		ScheduleJobCmd(JobId jobId, SerializedObject args, Executable executable) {
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

	class JobCompletedEvent implements Event {

		private final JobId jobId;
		private final Throwable exception;
		private final long executionDuration;
		private final Date completed;

		JobCompletedEvent(JobId jobId, Throwable exception, long executionDuration, Date completed) {
			this.jobId = jobId;
			this.exception = exception;
			this.executionDuration = executionDuration;
			this.completed = completed;
		}

		public JobId getJobId() {
			return jobId;
		}

		public Throwable getException() {
			return exception;
		}

		public long getExecutionDuration() {
			return executionDuration;
		}

		public Date getCompleted() {
			return completed;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			JobCompletedEvent that = (JobCompletedEvent) o;

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
			return "JobCompletedEvent{" +
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
