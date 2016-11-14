package ru.dorofeev.sandbox.quartzworkflow;

import ru.dorofeev.sandbox.quartzworkflow.QueueingOption.ExecutionType;
import rx.Observable;
import rx.subjects.PublishSubject;

import java.util.Optional;

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

	public static NotifyCompletedCmd notifyCompletedCmd(TaskId taskId) {
		return new NotifyCompletedCmd(taskId);
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

		private final TaskId taskId;

		NotifyCompletedCmd(TaskId taskId) {
			this.taskId = taskId;
		}

		TaskId getTaskId() {
			return taskId;
		}
	}

	private static class RequestNewTasksCmd implements Cmd { }

	public interface Event {

	}

	@SuppressWarnings("WeakerAccess")
	public static class TaskPoppedEvent implements Event {

		private final TaskId taskId;

		public TaskPoppedEvent(TaskId taskId) {
			this.taskId = taskId;
		}

		public TaskId getTaskId() {
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

	private final PublishSubject<Event> outputHolder = PublishSubject.create();
	private final PublishSubject<Exception> errorOutputHolder = PublishSubject.create();

	private final String name;
	private final QueueStore queueStore;

	public QueueManager(String name, QueueStore queueStore) {
		this.name = name;
		this.queueStore = queueStore;
	}

	public Observable<Exception> errors() {
		return errorOutputHolder;
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

		return outputHolder;
	}

	private void requestNewTasks() {
		tryPushNext(null);
	}

	private void notifyCompleted(NotifyCompletedCmd cmd) {
		Optional<String> queueName = queueStore.removeQueueItem(cmd.getTaskId());
		queueName.ifPresent(this::tryPushNext);
	}

	private void enqueue(EnqueueCmd cmd) {
		try {
			queueStore.insertQueueItem(cmd.getTaskId(), cmd.getQueueName(), cmd.getExecutionType());
			tryPushNext(cmd.getQueueName());
		} catch (QueueStoreException e) {
			errorOutputHolder.onNext(new EngineException(e.getMessage(), e));
		}
	}

	private void tryPushNext(String queueName) {
		Optional<TaskId> nextOpt = queueStore.getNextPendingQueueItem(queueName);
		nextOpt
			.map(TaskPoppedEvent::new)
			.ifPresent(tpe -> {
				outputHolder.onNext(tpe);
				tryPushNext(queueName);
			});
	}

	@Override
	public String toString() {
		return "QueueManager{" +
			"name='" + name + '\'' +
			'}';
	}
}
