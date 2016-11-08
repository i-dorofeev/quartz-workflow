package ru.dorofeev.sandbox.quartzworkflow;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static ru.dorofeev.sandbox.quartzworkflow.TaskRepository.EventType.ADD;
import static ru.dorofeev.sandbox.quartzworkflow.TaskRepository.EventType.COMPLETE;

class QueueManager {

	private final ObservableHolder<TaskId> outputHolder = new ObservableHolder<>();
	private final Map<String, TaskQueue> queues = new ConcurrentHashMap<>();

	rx.Observable<TaskId> bindEvents(rx.Observable<TaskRepository.Event> input) {
		input.subscribe(event -> {
			Task task = event.getTask();
			if (event.getEventType() == ADD)
				getQueue(task.getQueueName()).enqueue(task.getId(), task.getExecutionType());
			else if (event.getEventType() == COMPLETE)
				getQueue(task.getQueueName()).complete(task.getId());
		});

		return outputHolder.getObservable();
	}

	private TaskQueue getQueue(String name) {
		synchronized (queues) {
			TaskQueue taskQueue = queues.get(name);
			if (taskQueue != null)
				return taskQueue;

			taskQueue = new TaskQueue();
			queues.put(name, taskQueue);
			taskQueue.queue().subscribe(outputHolder);
			return taskQueue;
		}
	}
}
