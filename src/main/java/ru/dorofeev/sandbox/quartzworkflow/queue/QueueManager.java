package ru.dorofeev.sandbox.quartzworkflow.queue;

import org.springframework.util.Assert;
import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.NodeId;
import rx.Observable;

import static ru.dorofeev.sandbox.quartzworkflow.NodeId.ANY_NODE;

public interface QueueManager {

	String DEFAULT_QUEUE_NAME = "default";

	QueueingOptions.ExecutionType DEFAULT_EXECUTION_TYPE = QueueingOptions.ExecutionType.PARALLEL;

	Observable<Throwable> getErrors();

	rx.Observable<Event> bind(rx.Observable<Cmd> input);

	void suspend();

	void resume();

	interface Cmd { }

	interface Event {

	}

	static EnqueueCmd enqueueCmd(JobId jobId) {
		return new EnqueueCmd(DEFAULT_QUEUE_NAME, DEFAULT_EXECUTION_TYPE, jobId, ANY_NODE);
	}

	static EnqueueCmd enqueueCmd(QueueingOptions.ExecutionType executionType, JobId jobId) {
		return new EnqueueCmd(DEFAULT_QUEUE_NAME, executionType, jobId, ANY_NODE);
	}

	static EnqueueCmd enqueueCmd(String queueName, QueueingOptions.ExecutionType executionType, JobId jobId) {
		return new EnqueueCmd(queueName, executionType, jobId, ANY_NODE);
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
		private final NodeId nodeId;

		EnqueueCmd(String queueName, QueueingOptions.ExecutionType executionType, JobId jobId, NodeId nodeId) {
			Assert.notNull(nodeId, "nodeId shouldn't be null. Use NodeId.ANY_NODE constant instead.");

			this.queueName = queueName;
			this.executionType = executionType;
			this.jobId = jobId;
			this.nodeId = nodeId;
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

		NodeId getNodeId() {
			return nodeId;
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
