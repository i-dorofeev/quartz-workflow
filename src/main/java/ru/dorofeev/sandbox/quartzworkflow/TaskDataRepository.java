package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.JobDataMap;
import org.quartz.JobKey;
import rx.Subscriber;
import rx.functions.Func1;

import java.util.*;
import java.util.stream.Stream;

import static java.util.Optional.ofNullable;

public class TaskDataRepository {

	private Map<LocalId, TaskData> taskDataMap = new HashMap<>();
	private Map<LocalId, Set<LocalId>> childrenIndex = new HashMap<>();

	private void indexChild(LocalId parent, LocalId child) {
		Set<LocalId> children = childrenIndex.get(parent);
		if (children == null) {
			children = new HashSet<>();
			childrenIndex.put(parent, children);
		}

		children.add(child);
	}

	synchronized TaskData addTask(LocalId parentId, JobKey jobKey, JobDataMap jobDataMap) {
		if (parentId != null && !taskDataMap.containsKey(parentId))
			throw new EngineException("TaskData[id=" + parentId + "] not found");

		LocalId localId = LocalId.createUniqueLocalId();
		while (taskDataMap.containsKey(localId)) {
			localId = LocalId.createUniqueLocalId();
		}

		TaskData pd = new TaskData(localId, jobKey, jobDataMap);
		taskDataMap.put(localId, pd);

		if (parentId != null)
			indexChild(parentId, pd.getLocalId());

		return pd;
	}

	Optional<TaskData> findTaskData(LocalId localId) {
		return ofNullable(taskDataMap.get(localId));
	}

	@SuppressWarnings("WeakerAccess")
	public Stream<TaskData> traverse() {
		return taskDataMap.values().stream();
	}

	@SuppressWarnings("WeakerAccess")
	public rx.Observable<TaskData> traverse(LocalId rootId, Func1<? super TaskData, Boolean> predicate) {
		return rx.Observable.<TaskData>create(subscriber -> {
				traverse(rootId, subscriber);
				subscriber.onCompleted();
		}).filter(predicate);
	}

	private void traverse(LocalId rootId, Subscriber<? super TaskData> subscriber) {
		TaskData pd = taskDataMap.get(rootId);
		if (pd == null)
			subscriber.onError(new EngineException("Couldn't find taskData[id=" + rootId + "]"));
		else {
			subscriber.onNext(pd);
			ofNullable(childrenIndex.get(pd.getLocalId()))
				.ifPresent(children -> children.forEach(id -> traverse(id, subscriber)));
		}
	}

	public rx.Observable<TaskData> traverse(LocalId rootId, TaskData.Result result) {
		return traverse(rootId, taskData -> taskData.getResult().equals(result));
	}

	@SuppressWarnings("WeakerAccess")
	public Stream<TaskData> traverseFailed() {
		return traverse()
			.filter(pd -> pd.getResult().equals(TaskData.Result.FAILED));
	}


}
