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

public class TaskDataRepository {

	private final Map<TaskId, TaskData> taskDataMap = new HashMap<>();
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

	synchronized TaskData addTask(TaskId parentId, JobKey jobKey, JobDataMap jobDataMap, QueueingOption queueingOption) {
		if (parentId != null && !taskDataMap.containsKey(parentId))
			throw new EngineException("TaskData[id=" + parentId + "] not found");

		TaskId taskId = nextTaskId();
		String queueName = queueingOption != null ? queueingOption.getQueueName() : "default";

		TaskData td = new TaskData(taskId, queueName, jobKey, jobDataMap);
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
		TaskData taskData = ofNullable(taskDataMap.get(taskId))
			.orElseThrow(() -> new EngineException("Couldn't find taskDataRepository[id=" + taskId + "]"));

		taskData.recordResult(TaskData.Result.RUNNING, null);
	}

	void recordSuccess(TaskId taskId) {
		TaskData taskData = ofNullable(taskDataMap.get(taskId))
			.orElseThrow(() -> new EngineException("Couldn't find taskDataRepository[id=" + taskId + "]"));

		taskData.recordResult(TaskData.Result.SUCCESS, null);
		getQueue(taskData.getQueueName()).complete(taskData.getId());
	}

	void recordFailed(TaskId taskId, Throwable ex) {
		TaskData taskData = ofNullable(taskDataMap.get(taskId))
			.orElseThrow(() -> new EngineException("Couldn't find taskDataRepository[id=" + taskId + "]"));

		taskData.recordResult(TaskData.Result.FAILED, ex);
		getQueue(taskData.getQueueName()).complete(taskData.getId());
	}

	Optional<TaskData> findTaskData(TaskId taskId) {
		return ofNullable(taskDataMap.get(taskId));
	}

	@SuppressWarnings("WeakerAccess")
	public Stream<TaskData> traverse() {
		return taskDataMap.values().stream();
	}

	public rx.Observable<TaskData> traverse(TaskData.Result result) {
		return rx.Observable.<TaskData>create(s -> {
			taskDataMap.values().forEach(s::onNext);
			s.onCompleted();
		}).filter(td -> td.getResult() == result);
	}

	@SuppressWarnings("WeakerAccess")
	public rx.Observable<TaskData> traverse(TaskId rootId, Func1<? super TaskData, Boolean> predicate) {
		return rx.Observable.<TaskData>create(subscriber -> {
				traverse(rootId, subscriber);
				subscriber.onCompleted();
		}).filter(predicate);
	}

	private void traverse(TaskId rootId, Subscriber<? super TaskData> subscriber) {
		TaskData td = taskDataMap.get(rootId);
		if (td == null)
			subscriber.onError(new EngineException("Couldn't find taskData[id=" + rootId + "]"));
		else {
			subscriber.onNext(td);
			ofNullable(childrenIndex.get(td.getId()))
				.ifPresent(children -> children.forEach(id -> traverse(id, subscriber)));
		}
	}

	public rx.Observable<TaskData> traverse(TaskId rootId, TaskData.Result result) {
		return traverse(rootId, taskData -> taskData.getResult().equals(result));
	}

	@SuppressWarnings("WeakerAccess")
	public Stream<TaskData> traverseFailed() {
		return traverse()
			.filter(td -> td.getResult().equals(TaskData.Result.FAILED));
	}


}
