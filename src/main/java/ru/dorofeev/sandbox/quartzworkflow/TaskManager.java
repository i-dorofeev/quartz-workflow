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

	private final Map<TaskId, Task> taskDataMap = new HashMap<>();
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
		while (taskDataMap.containsKey(taskId)) {
			taskId = TaskId.createUniqueTaskId();
		}
		return taskId;
	}

	synchronized Task addTask(TaskId parentId, JobKey jobKey, JobDataMap jobDataMap, QueueingOption queueingOption) {
		if (parentId != null && !taskDataMap.containsKey(parentId))
			throw new EngineException("Task[id=" + parentId + "] not found");

		TaskId taskId = nextTaskId();
		String queueName = queueingOption != null ? queueingOption.getQueueName() : "default";

		Task td = new Task(taskId, queueName, jobKey, jobDataMap);
		taskDataMap.put(taskId, td);

		if (parentId != null)
			indexChild(parentId, td.getId());

		QueueingOption.ExecutionType executionType = queueingOption != null ? queueingOption.getExecutionType() : PARALLEL;

		getQueue(queueName).enqueue(taskId, executionType);
		return td;
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
		Task task = ofNullable(taskDataMap.get(taskId))
			.orElseThrow(() -> new EngineException("Couldn't find taskDataRepository[id=" + taskId + "]"));

		task.recordResult(Task.Result.RUNNING, null);
	}

	void recordSuccess(TaskId taskId) {
		Task task = ofNullable(taskDataMap.get(taskId))
			.orElseThrow(() -> new EngineException("Couldn't find taskDataRepository[id=" + taskId + "]"));

		task.recordResult(Task.Result.SUCCESS, null);
		getQueue(task.getQueueName()).complete(task.getId());
	}

	void recordFailed(TaskId taskId, Throwable ex) {
		Task task = ofNullable(taskDataMap.get(taskId))
			.orElseThrow(() -> new EngineException("Couldn't find taskDataRepository[id=" + taskId + "]"));

		task.recordResult(Task.Result.FAILED, ex);
		getQueue(task.getQueueName()).complete(task.getId());
	}

	Optional<Task> findTaskData(TaskId taskId) {
		return ofNullable(taskDataMap.get(taskId));
	}

	@SuppressWarnings("WeakerAccess")
	public Stream<Task> traverse() {
		return taskDataMap.values().stream();
	}

	public rx.Observable<Task> traverse(Task.Result result) {
		return rx.Observable.<Task>create(s -> {
			taskDataMap.values().forEach(s::onNext);
			s.onCompleted();
		}).filter(td -> td.getResult() == result);
	}

	@SuppressWarnings("WeakerAccess")
	public rx.Observable<Task> traverse(TaskId rootId, Func1<? super Task, Boolean> predicate) {
		return rx.Observable.<Task>create(subscriber -> {
				traverse(rootId, subscriber);
				subscriber.onCompleted();
		}).filter(predicate);
	}

	private void traverse(TaskId rootId, Subscriber<? super Task> subscriber) {
		Task td = taskDataMap.get(rootId);
		if (td == null)
			subscriber.onError(new EngineException("Couldn't find taskData[id=" + rootId + "]"));
		else {
			subscriber.onNext(td);
			ofNullable(childrenIndex.get(td.getId()))
				.ifPresent(children -> children.forEach(id -> traverse(id, subscriber)));
		}
	}

	public rx.Observable<Task> traverse(TaskId rootId, Task.Result result) {
		return traverse(rootId, taskData -> taskData.getResult().equals(result));
	}

	@SuppressWarnings("WeakerAccess")
	public Stream<Task> traverseFailed() {
		return traverse()
			.filter(td -> td.getResult().equals(Task.Result.FAILED));
	}


}
