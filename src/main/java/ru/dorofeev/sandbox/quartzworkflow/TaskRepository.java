package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.JobDataMap;
import org.quartz.JobKey;
import rx.Subscriber;
import rx.functions.Func1;

import java.util.*;
import java.util.stream.Stream;

import static java.util.Optional.ofNullable;
import static ru.dorofeev.sandbox.quartzworkflow.QueueingOption.ExecutionType.PARALLEL;
import static ru.dorofeev.sandbox.quartzworkflow.TaskRepository.EventType.ADD;
import static ru.dorofeev.sandbox.quartzworkflow.TaskRepository.EventType.COMPLETE;

public class TaskRepository {

	public static class Event {

		private final EventType eventType;
		private final Task task;

		private Event(EventType eventType, Task task) {
			this.eventType = eventType;
			this.task = task;
		}

		public EventType getEventType() {
			return eventType;
		}

		public Task getTask() {
			return task;
		}
	}

	public enum EventType { ADD, COMPLETE }

	private final Map<TaskId, Task> taskTable = new HashMap<>();
	private final Map<TaskId, Set<TaskId>> childrenIndex = new HashMap<>();
	private final ObservableHolder<Event> eventsHolder = new ObservableHolder<>();

	private void indexChild(TaskId parent, TaskId child) {
		Set<TaskId> children = childrenIndex.get(parent);
		if (children == null) {
			children = new HashSet<>();
			childrenIndex.put(parent, children);
		}

		children.add(child);
	}

	rx.Observable<Event> events() {
		return eventsHolder.getObservable();
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
		QueueingOption.ExecutionType executionType = queueingOption != null ? queueingOption.getExecutionType() : PARALLEL;

		Task t = new Task(taskId, queueName, executionType, jobKey, jobDataMap);
		taskTable.put(taskId, t);

		if (parentId != null)
			indexChild(parentId, t.getId());

		eventsHolder.onNext(new Event(ADD, t));
		return t;
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
		eventsHolder.onNext(new Event(COMPLETE, task));
	}

	void recordFailed(TaskId taskId, Throwable ex) {
		Task task = ofNullable(taskTable.get(taskId))
			.orElseThrow(() -> new EngineException("Couldn't find task[id=" + taskId + "]"));

		task.recordResult(Task.Result.FAILED, ex);
		eventsHolder.onNext(new Event(COMPLETE, task));
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
