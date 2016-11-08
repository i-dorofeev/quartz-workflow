package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.JobDataMap;
import org.quartz.JobKey;
import rx.Subscriber;
import rx.functions.Func1;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static java.util.Optional.ofNullable;
import static ru.dorofeev.sandbox.quartzworkflow.QueueingOption.ExecutionType.PARALLEL;

public class TaskManager {

	private final Map<TaskId, Task> taskTable = new HashMap<>();
	private final Map<TaskId, Set<TaskId>> childrenIndex = new HashMap<>();
	private final ObservableHolder<TaskId> executionFlowObservableHolder = new ObservableHolder<>();

	private final Map<String, TaskQueue> queues = new ConcurrentHashMap<>();

	private void indexChild(TaskId parent, TaskId child) {
		Set<TaskId> children = childrenIndex.get(parent);
		if (children == null) {
			children = new HashSet<>();
			childrenIndex.put(parent, children);
		}

		children.add(child);
	}

	rx.Observable<TaskId> executionTaskFlow() {
		return executionFlowObservableHolder.getObservable();
	}

	private TaskId nextTaskId() {
		TaskId taskId = TaskId.createUniqueTaskId();
		while (taskTable.containsKey(taskId)) {
			taskId = TaskId.createUniqueTaskId();
		}
		return taskId;
	}

	synchronized Task addTask(TaskId parentId, JobKey jobKey, JobDataMap jobDataMap, QueueingOption queueingOption) {
		if (parentId != null && !taskTable.containsKey(parentId))
			throw new EngineException("Task[id=" + parentId + "] not found");

		TaskId taskId = nextTaskId();
		String queueName = queueingOption != null ? queueingOption.getQueueName() : "default";

		Task t = new Task(taskId, queueName, jobKey, jobDataMap);
		taskTable.put(taskId, t);

		if (parentId != null)
			indexChild(parentId, t.getId());

		QueueingOption.ExecutionType executionType = queueingOption != null ? queueingOption.getExecutionType() : PARALLEL;

		getQueue(queueName).enqueue(taskId, executionType);
		return t;
	}

	private TaskQueue getQueue(String name) {
		synchronized (queues) {
			TaskQueue taskQueue = queues.get(name);
			if (taskQueue != null)
				return taskQueue;

			taskQueue = new TaskQueue();
			queues.put(name, taskQueue);
			taskQueue.queue().subscribe(executionFlowObservableHolder);
			return taskQueue;
		}
	}

	void recordRunning(TaskId taskId) {
		Task task = ofNullable(taskTable.get(taskId))
			.orElseThrow(() -> new EngineException("Couldn't find task[id=" + taskId + "]"));

		task.recordResult(Task.Result.RUNNING, null);
	}

	void recordSuccess(TaskId taskId) {
		Task task = ofNullable(taskTable.get(taskId))
			.orElseThrow(() -> new EngineException("Couldn't find task[id=" + taskId + "]"));

		task.recordResult(Task.Result.SUCCESS, null);
		getQueue(task.getQueueName()).complete(task.getId());
	}

	void recordFailed(TaskId taskId, Throwable ex) {
		Task task = ofNullable(taskTable.get(taskId))
			.orElseThrow(() -> new EngineException("Couldn't find task[id=" + taskId + "]"));

		task.recordResult(Task.Result.FAILED, ex);
		getQueue(task.getQueueName()).complete(task.getId());
	}

	Optional<Task> findTask(TaskId taskId) {
		return ofNullable(taskTable.get(taskId));
	}

	@SuppressWarnings("WeakerAccess")
	public Stream<Task> traverse() {
		return taskTable.values().stream();
	}

	public rx.Observable<Task> traverse(Task.Result result) {
		return rx.Observable.<Task>create(s -> {
			taskTable.values().forEach(s::onNext);
			s.onCompleted();
		}).filter(t -> t.getResult() == result);
	}

	@SuppressWarnings("WeakerAccess")
	public rx.Observable<Task> traverse(TaskId rootId, Func1<? super Task, Boolean> predicate) {
		return rx.Observable.<Task>create(subscriber -> {
				traverse(rootId, subscriber);
				subscriber.onCompleted();
		}).filter(predicate);
	}

	private void traverse(TaskId rootId, Subscriber<? super Task> subscriber) {
		Task t = taskTable.get(rootId);
		if (t == null)
			subscriber.onError(new EngineException("Couldn't find task[id=" + rootId + "]"));
		else {
			subscriber.onNext(t);
			ofNullable(childrenIndex.get(t.getId()))
				.ifPresent(children -> children.forEach(id -> traverse(id, subscriber)));
		}
	}

	public rx.Observable<Task> traverse(TaskId rootId, Task.Result result) {
		return traverse(rootId, task -> task.getResult().equals(result));
	}

	@SuppressWarnings("WeakerAccess")
	public Stream<Task> traverseFailed() {
		return traverse()
			.filter(t -> t.getResult().equals(Task.Result.FAILED));
	}


}
