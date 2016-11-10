package ru.dorofeev.sandbox.quartzworkflow;

import ru.dorofeev.sandbox.quartzworkflow.QueueingOption.ExecutionType;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static ru.dorofeev.sandbox.quartzworkflow.QueueingOption.ExecutionType.PARALLEL;

public class QueueManager {

	@SuppressWarnings("WeakerAccess")
	public static final String DEFAULT_QUEUE_NAME = "default";

	@SuppressWarnings("WeakerAccess")
	public static final ExecutionType DEFAULT_EXECUTION_TYPE = PARALLEL;

	public static EnqueueCmd enqueueCmd(TaskId taskId) {
		return new EnqueueCmd(DEFAULT_QUEUE_NAME, DEFAULT_EXECUTION_TYPE, taskId);
	}

	public static EnqueueCmd enqueueCmd(ExecutionType executionType, TaskId taskId) {
		return new EnqueueCmd(DEFAULT_QUEUE_NAME, executionType, taskId);
	}

	public static TaskPoppedEvent taskPoppedEvent(TaskId taskId) {
		return new TaskPoppedEvent(taskId);
	}

	public interface Cmd { }

	@SuppressWarnings("WeakerAccess")
	public static class EnqueueCmd implements Cmd {

		private final String queueName;
		private final ExecutionType executionType;
		private final TaskId taskId;

		public EnqueueCmd(String queueName, ExecutionType executionType, TaskId taskId) {
			this.queueName = queueName;
			this.executionType = executionType;
			this.taskId = taskId;
		}


		String getQueueName() {
			return queueName;
		}

		ExecutionType getExecutionType() {
			return executionType;
		}

		TaskId getTaskId() {
			return taskId;
		}
	}

	static class NotifyCompletedCmd implements Cmd {

		private final String queueName;
		private final TaskId taskId;

		NotifyCompletedCmd(String queueName, TaskId taskId) {
			this.queueName = queueName;
			this.taskId = taskId;
		}

		String getQueueName() {
			return queueName;
		}

		TaskId getTaskId() {
			return taskId;
		}
	}

	public interface Event {

	}

	@SuppressWarnings("WeakerAccess")
	public static class TaskPoppedEvent implements Event {

		private final TaskId taskId;

		public TaskPoppedEvent(TaskId taskId) {
			this.taskId = taskId;
		}

		TaskId getTaskId() {
			return taskId;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TaskPoppedEvent that = (TaskPoppedEvent) o;

			return taskId.equals(that.taskId);

		}

		@Override
		public int hashCode() {
			return taskId.hashCode();
		}
	}

	private final ObservableHolder<Event> outputHolder = new ObservableHolder<>();
	private final Map<String, TaskQueue> queues = new ConcurrentHashMap<>();

	public rx.Observable<Event> bindEvents(rx.Observable<Cmd> input) {
		input.subscribe(cmd -> {

			if (cmd instanceof EnqueueCmd)
				enqueue((EnqueueCmd) cmd);

			else if (cmd instanceof NotifyCompletedCmd)
				notifyCompleted((NotifyCompletedCmd) cmd);

			else
				throw new EngineException("Unrecognized cmd " + cmd);
		});

		return outputHolder.getObservable();
	}

	private void notifyCompleted(NotifyCompletedCmd cmd) {
		getQueue(cmd.getQueueName()).complete(cmd.getTaskId());
	}

	private void enqueue(EnqueueCmd cmd) {
		getQueue(cmd.getQueueName()).enqueue(cmd.getTaskId(), cmd.getExecutionType());
	}

	private TaskQueue getQueue(String name) {
		synchronized (queues) {
			TaskQueue taskQueue = queues.get(name);
			if (taskQueue != null)
				return taskQueue;

			taskQueue = new TaskQueue();
			queues.put(name, taskQueue);
			taskQueue.queue()
				.map(TaskPoppedEvent::new)
				.subscribe(outputHolder);
			return taskQueue;
		}
	}
}
