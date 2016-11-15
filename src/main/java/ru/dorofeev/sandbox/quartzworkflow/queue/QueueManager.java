package ru.dorofeev.sandbox.quartzworkflow.queue;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import rx.Observable;

public interface QueueManager {

	String DEFAULT_QUEUE_NAME = "default";

	QueueingOption.ExecutionType DEFAULT_EXECUTION_TYPE = QueueingOption.ExecutionType.PARALLEL;

	Observable<Throwable> getErrors();

	rx.Observable<Event> bind(rx.Observable<Cmd> input);

	interface Cmd { }

	interface Event {

	}

	static EnqueueCmd enqueueCmd(JobId jobId) {
		return new EnqueueCmd(DEFAULT_QUEUE_NAME, DEFAULT_EXECUTION_TYPE, jobId);
	}

	static EnqueueCmd enqueueCmd(QueueingOption.ExecutionType executionType, JobId jobId) {
		return new EnqueueCmd(DEFAULT_QUEUE_NAME, executionType, jobId);
	}

	static EnqueueCmd enqueueCmd(String queueName, QueueingOption.ExecutionType executionType, JobId jobId) {
		return new EnqueueCmd(queueName, executionType, jobId);
	}

	static NotifyCompletedCmd notifyCompletedCmd(JobId jobId) {
		return new NotifyCompletedCmd(jobId);
	}

	static TaskPoppedEvent taskPoppedEvent(JobId jobId) {
		return new TaskPoppedEvent(jobId);
	}

	static GiveMeMoreCmd giveMeMoreCmd() {
		return new GiveMeMoreCmd();
	}

	class EnqueueCmd implements Cmd {

		private final String queueName;
		private final QueueingOption.ExecutionType executionType;
		private final JobId jobId;

		EnqueueCmd(String queueName, QueueingOption.ExecutionType executionType, JobId jobId) {
			this.queueName = queueName;
			this.executionType = executionType;
			this.jobId = jobId;
		}


		String getQueueName() {
			return queueName;
		}

		QueueingOption.ExecutionType getExecutionType() {
			return executionType;
		}

		JobId getJobId() {
			return jobId;
		}
	}

	class NotifyCompletedCmd implements Cmd {

		private final JobId jobId;

		NotifyCompletedCmd(JobId jobId) {
			this.jobId = jobId;
		}

		JobId getJobId() {
			return jobId;
		}
	}

	class GiveMeMoreCmd implements Cmd { }

	class TaskPoppedEvent implements Event {

		private final JobId jobId;

		TaskPoppedEvent(JobId jobId) {
			this.jobId = jobId;
		}

		public JobId getJobId() {
			return jobId;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			QueueManager.TaskPoppedEvent that = (TaskPoppedEvent) o;

			return jobId.equals(that.jobId);
		}

		@Override
		public int hashCode() {
			return jobId.hashCode();
		}

		@Override
		public String toString() {
			return "TaskPoppedEvent{" +
				"jobId=" + jobId +
				'}';
		}
	}
}
