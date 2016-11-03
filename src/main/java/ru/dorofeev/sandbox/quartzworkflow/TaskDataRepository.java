package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.JobDataMap;
import org.quartz.JobKey;
import rx.Subscriber;
import rx.functions.Func1;

import java.util.*;
import java.util.stream.Stream;

import static java.util.Optional.ofNullable;

public class TaskDataRepository {

	private Map<TaskId, TaskData> taskDataMap = new HashMap<>();
	private Map<TaskId, Set<TaskId>> childrenIndex = new HashMap<>();

	private void indexChild(TaskId parent, TaskId child) {
		Set<TaskId> children = childrenIndex.get(parent);
		if (children == null) {
			children = new HashSet<>();
			childrenIndex.put(parent, children);
		}

		children.add(child);
	}

	synchronized TaskData addTask(TaskId parentId, JobKey jobKey, JobDataMap jobDataMap) {
		if (parentId != null && !taskDataMap.containsKey(parentId))
			throw new EngineException("TaskData[id=" + parentId + "] not found");

		TaskId taskId = TaskId.createUniqueTaskId();
		while (taskDataMap.containsKey(taskId)) {
			taskId = TaskId.createUniqueTaskId();
		}

		TaskData pd = new TaskData(taskId, jobKey, jobDataMap);
		taskDataMap.put(taskId, pd);

		if (parentId != null)
			indexChild(parentId, pd.getTaskId());

		return pd;
	}

	Optional<TaskData> findTaskData(TaskId taskId) {
		return ofNullable(taskDataMap.get(taskId));
	}

	@SuppressWarnings("WeakerAccess")
	public Stream<TaskData> traverse() {
		return taskDataMap.values().stream();
	}

	@SuppressWarnings("WeakerAccess")
	public rx.Observable<TaskData> traverse(TaskId rootId, Func1<? super TaskData, Boolean> predicate) {
		return rx.Observable.<TaskData>create(subscriber -> {
				traverse(rootId, subscriber);
				subscriber.onCompleted();
		}).filter(predicate);
	}

	private void traverse(TaskId rootId, Subscriber<? super TaskData> subscriber) {
		TaskData pd = taskDataMap.get(rootId);
		if (pd == null)
			subscriber.onError(new EngineException("Couldn't find taskData[id=" + rootId + "]"));
		else {
			subscriber.onNext(pd);
			ofNullable(childrenIndex.get(pd.getTaskId()))
				.ifPresent(children -> children.forEach(id -> traverse(id, subscriber)));
		}
	}

	public rx.Observable<TaskData> traverse(TaskId rootId, TaskData.Result result) {
		return traverse(rootId, taskData -> taskData.getResult().equals(result));
	}

	@SuppressWarnings("WeakerAccess")
	public Stream<TaskData> traverseFailed() {
		return traverse()
			.filter(pd -> pd.getResult().equals(TaskData.Result.FAILED));
	}


}
