package ru.dorofeev.sandbox.quartzworkflow.taskrepo;

import ru.dorofeev.sandbox.quartzworkflow.JobDataMap;
import ru.dorofeev.sandbox.quartzworkflow.JobKey;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOption;
import ru.dorofeev.sandbox.quartzworkflow.TaskId;
import ru.dorofeev.sandbox.quartzworkflow.engine.EngineException;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

import java.util.*;
import java.util.stream.Stream;

import static java.util.Optional.ofNullable;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOption.ExecutionType.PARALLEL;

class TaskRepositoryImpl implements TaskRepository {

	private final Map<TaskId, Task> taskTable = new HashMap<>();
	private final Map<TaskId, Set<TaskId>> childrenIndex = new HashMap<>();
	private final PublishSubject<Event> eventsHolder = PublishSubject.create();

	private void indexChild(TaskId parent, TaskId child) {
		Set<TaskId> children = childrenIndex.get(parent);
		if (children == null) {
			children = new HashSet<>();
			childrenIndex.put(parent, children);
		}

		children.add(child);
	}

	@Override
	public rx.Observable<Event> bind(Observable<Cmd> input) {

		input.ofType(AddTaskCmd.class)
			.map(cmd -> addTaskInternal(cmd.getParentId(), cmd.getJobKey(), cmd.getJobDataMap(), cmd.getQueueingOption()))
			.subscribe(this.eventsHolder);

		input.ofType(CompleteTaskCmd.class)
			.map(cmd -> {
				Task task = ofNullable(taskTable.get(cmd.getTaskId()))
					.orElseThrow(() -> new EngineException("Couldn't find task[id=" + cmd.getTaskId() + "]"));

				if (cmd.getException() != null)
					task.recordResult(Task.Result.FAILED, cmd.getException());
				else
					task.recordResult(Task.Result.SUCCESS, null);

				return new Event(EventType.COMPLETE, task);
			})
			.subscribe(eventsHolder);

		return eventsHolder;
	}

	private TaskId nextTaskId() {
		TaskId taskId = TaskId.createUniqueTaskId();
		while (taskTable.containsKey(taskId)) {
			taskId = TaskId.createUniqueTaskId();
		}
		return taskId;
	}

	@Override
	public Task addTask(TaskId parentId, JobKey jobKey, JobDataMap jobDataMap, QueueingOption queueingOption) {
		Event event = addTaskInternal(parentId, jobKey, jobDataMap, queueingOption);
		eventsHolder.onNext(event);
		return event.getTask();
	}

	private synchronized Event addTaskInternal(TaskId parentId, JobKey jobKey, JobDataMap jobDataMap, QueueingOption queueingOption) {
		if (parentId != null && !taskTable.containsKey(parentId))
			throw new EngineException("Task[id=" + parentId + "] not found");

		TaskId taskId = nextTaskId();
		String queueName = queueingOption != null ? queueingOption.getQueueName() : "default";
		QueueingOption.ExecutionType executionType = queueingOption != null ? queueingOption.getExecutionType() : PARALLEL;

		Task t = new Task(taskId, queueName, executionType, jobKey, jobDataMap);
		taskTable.put(taskId, t);

		if (parentId != null)
			indexChild(parentId, t.getId());

		return new Event(EventType.ADD, t);
	}

	@Override
	public Optional<Task> findTask(TaskId taskId) {
		return ofNullable(taskTable.get(taskId));
	}

	@Override
	public Stream<Task> traverse() {
		return taskTable.values().stream();
	}

	@Override
	public rx.Observable<Task> traverse(Task.Result result) {
		return rx.Observable.<Task>create(s -> {
			taskTable.values().forEach(s::onNext);
			s.onCompleted();
		}).filter(t -> t.getResult() == result);
	}

	@Override
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

	@Override
	public rx.Observable<Task> traverse(TaskId rootId, Task.Result result) {
		return traverse(rootId, task -> task.getResult().equals(result));
	}

	@Override
	public Stream<Task> traverseFailed() {
		return traverse()
			.filter(t -> t.getResult().equals(Task.Result.FAILED));
	}
}
