package ru.dorofeev.sandbox.quartzworkflow;

import ru.dorofeev.sandbox.quartzworkflow.QueueingOption.ExecutionType;
import rx.Observable;

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

	public static NotifyCompletedCmd notifyCompletedCmd(String queueName, TaskId taskId) {
		return new NotifyCompletedCmd(queueName, taskId);
	}

	public static TaskPoppedEvent taskPoppedEvent(TaskId taskId) {
		return new TaskPoppedEvent(taskId);
	}

	public static RequestNewTasksCmd requestNewTasksCmd() {
		return new RequestNewTasksCmd();
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

	static class RequestNewTasksCmd implements Cmd { }

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

		@Override
		public String toString() {
			return "TaskPoppedEvent{" +
				"taskId=" + taskId +
				'}';
		}
	}

	private final ObservableHolder<Event> outputHolder = new ObservableHolder<>();
	private final ObservableHolder<Exception> errorOutputHolder = new ObservableHolder<>();
	private final Map<String, TaskQueue> queues = new ConcurrentHashMap<>();
	private final QueueStore queueStore;

	public QueueManager(QueueStore queueStore) {
		this.queueStore = queueStore;
	}

	public Observable<Exception> errors() {
		return errorOutputHolder.getObservable();
	}

	public rx.Observable<Event> bindEvents(rx.Observable<Cmd> input) {
		input.subscribe(cmd -> {

			if (cmd instanceof EnqueueCmd)
				enqueue((EnqueueCmd) cmd);

			else if (cmd instanceof NotifyCompletedCmd)
				notifyCompleted((NotifyCompletedCmd) cmd);

			else if (cmd instanceof RequestNewTasksCmd)
				requestNewTasks();

			else
				errorOutputHolder.onNext(new EngineException("Unrecognized cmd " + cmd));
		});

		return outputHolder.getObservable();
	}

	private void requestNewTasks() {
		queues.values().forEach(TaskQueue::tryPushNext);
	}

	private void notifyCompleted(NotifyCompletedCmd cmd) {
		getQueue(cmd.getQueueName()).complete(cmd.getTaskId());
	}

	private void enqueue(EnqueueCmd cmd) {
		try {
			getQueue(cmd.getQueueName()).enqueue(cmd.getTaskId(), cmd.getExecutionType());
		} catch (QueueStoreException e) {
			errorOutputHolder.onNext(new EngineException(e.getMessage(), e));
		}
	}

	private TaskQueue getQueue(String name) {
		synchronized (queues) {
			TaskQueue taskQueue = queues.get(name);
			if (taskQueue != null)
				return taskQueue;

			taskQueue = new TaskQueue(queueStore, name);
			queues.put(name, taskQueue);
			taskQueue.queue()
				.map(TaskPoppedEvent::new)
				.subscribe(outputHolder);
			return taskQueue;
		}
	}
}
