package ru.dorofeev.sandbox.quartzworkflow.queue;

import ru.dorofeev.sandbox.quartzworkflow.TaskId;
import rx.Observable;

public interface QueueManager {

	String DEFAULT_QUEUE_NAME = "default";

	QueueingOption.ExecutionType DEFAULT_EXECUTION_TYPE = QueueingOption.ExecutionType.PARALLEL;

	Observable<Exception> errors();

	rx.Observable<Event> bind(rx.Observable<Cmd> input);

	interface Cmd { }

	interface Event {

	}

	static EnqueueCmd enqueueCmd(TaskId taskId) {
		return new EnqueueCmd(DEFAULT_QUEUE_NAME, DEFAULT_EXECUTION_TYPE, taskId);
	}

	static EnqueueCmd enqueueCmd(QueueingOption.ExecutionType executionType, TaskId taskId) {
		return new EnqueueCmd(DEFAULT_QUEUE_NAME, executionType, taskId);
	}

	static NotifyCompletedCmd notifyCompletedCmd(TaskId taskId) {
		return new NotifyCompletedCmd(taskId);
	}

	static TaskPoppedEvent taskPoppedEvent(TaskId taskId) {
		return new TaskPoppedEvent(taskId);
	}

	static GiveMeMoreCmd giveMeMoreCmd() {
		return new GiveMeMoreCmd();
	}

	class EnqueueCmd implements Cmd {

		private final String queueName;
		private final QueueingOption.ExecutionType executionType;
		private final TaskId taskId;

		public EnqueueCmd(String queueName, QueueingOption.ExecutionType executionType, TaskId taskId) {
			this.queueName = queueName;
			this.executionType = executionType;
			this.taskId = taskId;
		}


		String getQueueName() {
			return queueName;
		}

		QueueingOption.ExecutionType getExecutionType() {
			return executionType;
		}

		TaskId getTaskId() {
			return taskId;
		}
	}

	class NotifyCompletedCmd implements Cmd {

		private final TaskId taskId;

		public NotifyCompletedCmd(TaskId taskId) {
			this.taskId = taskId;
		}

		TaskId getTaskId() {
			return taskId;
		}
	}

	class GiveMeMoreCmd implements Cmd { }

	class TaskPoppedEvent implements Event {

		private final TaskId taskId;

		TaskPoppedEvent(TaskId taskId) {
			this.taskId = taskId;
		}

		public TaskId getTaskId() {
			return taskId;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			QueueManager.TaskPoppedEvent that = (TaskPoppedEvent) o;

			return taskId.equals(that.taskId);

		}

		@Override
		public int hashCode() {
			return taskId.hashCode();
		}

		@Override
		public String toString() {
			return "TaskPoppedEvent{" +
				"taskId=" + taskId +
				'}';
		}
	}
}
