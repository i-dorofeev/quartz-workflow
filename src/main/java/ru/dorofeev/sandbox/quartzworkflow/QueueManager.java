package ru.dorofeev.sandbox.quartzworkflow;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class QueueManager {

	interface Cmd { }

	static class EnqueueCmd implements Cmd {

		private final String queueName;
		private final QueueingOption.ExecutionType executionType;
		private final TaskId taskId;

		EnqueueCmd(String queueName, QueueingOption.ExecutionType executionType, TaskId taskId) {
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

	interface Event {

	}

	static class TaskPoppedEvent implements Event {

		private final TaskId taskId;

		TaskPoppedEvent(TaskId taskId) {
			this.taskId = taskId;
		}

		TaskId getTaskId() {
			return taskId;
		}
	}

	private final ObservableHolder<Event> outputHolder = new ObservableHolder<>();
	private final Map<String, TaskQueue> queues = new ConcurrentHashMap<>();

	rx.Observable<Event> bindEvents(rx.Observable<Cmd> input) {
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
