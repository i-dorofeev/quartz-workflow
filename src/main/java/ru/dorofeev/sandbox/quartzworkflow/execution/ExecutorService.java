package ru.dorofeev.sandbox.quartzworkflow.execution;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.NodeId;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObject;
import rx.Observable;

import java.util.Date;

public interface ExecutorService {

	static ScheduleJobCmd scheduleJobCmd(JobId jobId, SerializedObject args, Executable runnable) {
		return new ScheduleJobCmd(jobId, args, runnable);
	}

	static JobCompletedEvent jobSuccessfullyCompletedEvent(JobId jobId, long executionDuration, Date completed, NodeId completedNodeId) {
		return new JobCompletedEvent(jobId, null, executionDuration, completed, completedNodeId);
	}

	static JobCompletedEvent jobFailedEvent(JobId jobId, Throwable e, long executionDuration, Date completed, NodeId completedNodeId) {
		return new JobCompletedEvent(jobId, e, executionDuration, completed, completedNodeId);
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
		private final NodeId completedNodeId;

		JobCompletedEvent(JobId jobId, Throwable exception, long executionDuration, Date completed, NodeId completedNodeId) {
			this.jobId = jobId;
			this.exception = exception;
			this.executionDuration = executionDuration;
			this.completed = completed;
			this.completedNodeId = completedNodeId;
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

			if (executionDuration != that.executionDuration) return false;
			if (jobId != null ? !jobId.equals(that.jobId) : that.jobId != null) return false;
			if (exception != null ? !exception.equals(that.exception) : that.exception != null) return false;
			return completed != null ? completed.equals(that.completed) : that.completed == null;
		}

		@Override
		public int hashCode() {
			int result = jobId != null ? jobId.hashCode() : 0;
			result = 31 * result + (exception != null ? exception.hashCode() : 0);
			result = 31 * result + (int) (executionDuration ^ (executionDuration >>> 32));
			result = 31 * result + (completed != null ? completed.hashCode() : 0);
			return result;
		}

		@Override
		public String toString() {
			return "JobCompletedEvent{" +
				"jobId=" + jobId +
				", exception=" + exception +
				", executionDuration=" + executionDuration +
				", completed=" + completed +
				", completedNodeId=" + completedNodeId +
				'}';
		}

		public NodeId getCompletedNodeId() {
			return completedNodeId;
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
