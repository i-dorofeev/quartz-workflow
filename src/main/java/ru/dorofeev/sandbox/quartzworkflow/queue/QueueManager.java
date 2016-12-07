package ru.dorofeev.sandbox.quartzworkflow.queue;

import org.springframework.util.Assert;
import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.NodeSpecification;
import rx.Observable;

public interface QueueManager {

	Observable<Throwable> getErrors();

	rx.Observable<Event> bind(rx.Observable<Cmd> input);

	void suspend();

	void resume();

	interface Cmd { }

	interface Event {

	}

	static EnqueueCmd enqueueCmd(String queueName, QueueingOptions.ExecutionType executionType, JobId jobId, NodeSpecification nodeSpecification) {
		return new EnqueueCmd(queueName, executionType, jobId, nodeSpecification);
	}

	static NotifyCompletedCmd notifyCompletedCmd(JobId jobId) {
		return new NotifyCompletedCmd(jobId);
	}

	static JobPoppedEvent jobPoppedEvent(JobId jobId) {
		return new JobPoppedEvent(jobId);
	}

	static GiveMeMoreCmd giveMeMoreCmd() {
		return new GiveMeMoreCmd();
	}

	class EnqueueCmd implements Cmd {

		private final String queueName;
		private final QueueingOptions.ExecutionType executionType;
		private final JobId jobId;
		private final NodeSpecification nodeSpecification;

		EnqueueCmd(String queueName, QueueingOptions.ExecutionType executionType, JobId jobId, NodeSpecification nodeSpecification) {
			Assert.notNull(nodeSpecification, "nodeSpecification shouldn't be null. Use NodeSpecification.ANY_NODE constant to match any node.");

			this.queueName = queueName;
			this.executionType = executionType;
			this.jobId = jobId;
			this.nodeSpecification = nodeSpecification;
		}

		String getQueueName() {
			return queueName;
		}

		QueueingOptions.ExecutionType getExecutionType() {
			return executionType;
		}

		JobId getJobId() {
			return jobId;
		}

		NodeSpecification getNodeSpecification() {
			return nodeSpecification;
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

	class JobPoppedEvent implements Event {

		private final JobId jobId;

		JobPoppedEvent(JobId jobId) {
			this.jobId = jobId;
		}

		public JobId getJobId() {
			return jobId;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			JobPoppedEvent that = (JobPoppedEvent) o;

			return jobId.equals(that.jobId);
		}

		@Override
		public int hashCode() {
			return jobId.hashCode();
		}

		@Override
		public String toString() {
			return "JobPoppedEvent{" +
				"jobId=" + jobId +
				'}';
		}
	}
}
