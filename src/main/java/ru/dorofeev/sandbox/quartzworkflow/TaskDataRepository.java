package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.JobDataMap;
import org.quartz.JobKey;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

import java.util.*;
import java.util.stream.Stream;

import static java.util.Optional.ofNullable;

public class TaskDataRepository {

	private Map<TaskId, TaskData> taskDataMap = new HashMap<>();
	private Map<TaskId, Set<TaskId>> childrenIndex = new HashMap<>();
	private ArrayList<Subscriber<? super TaskData>> executionTaskFlowSubscribers = new ArrayList<>();

	private void indexChild(TaskId parent, TaskId child) {
		Set<TaskId> children = childrenIndex.get(parent);
		if (children == null) {
			children = new HashSet<>();
			childrenIndex.put(parent, children);
		}

		children.add(child);
	}

	rx.Observable<TaskData> executionTaskFlow() {
		return Observable.create(subscriber -> executionTaskFlowSubscribers.add(subscriber));
	}

	synchronized TaskData addTask(TaskId parentId, JobKey jobKey, JobDataMap jobDataMap) {
		if (parentId != null && !taskDataMap.containsKey(parentId))
			throw new EngineException("TaskData[id=" + parentId + "] not found");

		TaskId taskId = TaskId.createUniqueTaskId();
		while (taskDataMap.containsKey(taskId)) {
			taskId = TaskId.createUniqueTaskId();
		}

		TaskData td = new TaskData(taskId, jobKey, jobDataMap);
		taskDataMap.put(taskId, td);

		if (parentId != null)
			indexChild(parentId, td.getTaskId());

		executionTaskFlowSubscribers.forEach(s -> s.onNext(td));
		return td;
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
			ofNullable(childrenIndex.get(td.getTaskId()))
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
