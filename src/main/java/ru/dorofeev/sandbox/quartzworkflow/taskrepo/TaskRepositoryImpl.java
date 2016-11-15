package ru.dorofeev.sandbox.quartzworkflow.taskrepo;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.JobKey;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOption;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObject;
import ru.dorofeev.sandbox.quartzworkflow.utils.ErrorObservable;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

import java.util.*;
import java.util.stream.Stream;

import static java.util.Optional.ofNullable;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOption.ExecutionType.PARALLEL;

class TaskRepositoryImpl implements TaskRepository {

	private final Map<JobId, Task> taskTable = new HashMap<>();
	private final Map<JobId, Set<JobId>> childrenIndex = new HashMap<>();
	private final PublishSubject<Event> events = PublishSubject.create();
	private final ErrorObservable errors = new ErrorObservable();

	private void indexChild(JobId parent, JobId child) {
		Set<JobId> children = childrenIndex.get(parent);
		if (children == null) {
			children = new HashSet<>();
			childrenIndex.put(parent, children);
		}

		children.add(child);
	}

	@Override
	public rx.Observable<Event> bind(Observable<Cmd> input) {

		input.ofType(AddTaskCmd.class)
			.compose(errors.mapRetry(cmd -> addTaskInternal(cmd.getParentId(), cmd.getJobKey(), cmd.getArgs(), cmd.getQueueingOption())))
			.subscribe(this.events);

		input.ofType(CompleteTaskCmd.class)
			.compose(errors.mapRetry(cmd -> {
				Task task = ofNullable(taskTable.get(cmd.getJobId()))
					.orElseThrow(() -> new TaskRepositoryException("Couldn't find task[id=" + cmd.getJobId() + "]"));

				if (cmd.getException() != null)
					task.recordResult(Task.Result.FAILED, cmd.getException());
				else
					task.recordResult(Task.Result.SUCCESS, null);

				return new Event(EventType.COMPLETE, task);
			}))
			.subscribe(events);

		return events;
	}

	@Override
	public Observable<Throwable> getErrors() {
		return errors.asObservable();
	}

	private JobId nextTaskId() {
		JobId jobId = JobId.createUniqueTaskId();
		while (taskTable.containsKey(jobId)) {
			jobId = JobId.createUniqueTaskId();
		}
		return jobId;
	}

	@Override
	public Task addTask(JobId parentId, JobKey jobKey, SerializedObject args, QueueingOption queueingOption) {
		Event event = addTaskInternal(parentId, jobKey, args, queueingOption);
		events.onNext(event);
		return event.getTask();
	}

	private synchronized Event addTaskInternal(JobId parentId, JobKey jobKey, SerializedObject args, QueueingOption queueingOption) {
		if (parentId != null && !taskTable.containsKey(parentId))
			throw new TaskRepositoryException("Task[id=" + parentId + "] not found");

		JobId jobId = nextTaskId();
		String queueName = queueingOption != null ? queueingOption.getQueueName() : "default";
		QueueingOption.ExecutionType executionType = queueingOption != null ? queueingOption.getExecutionType() : PARALLEL;

		Task t = new Task(jobId, queueName, executionType, jobKey, args);
		taskTable.put(jobId, t);

		if (parentId != null)
			indexChild(parentId, t.getId());

		return new Event(EventType.ADD, t);
	}

	@Override
	public Optional<Task> findTask(JobId jobId) {
		return ofNullable(taskTable.get(jobId));
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
	public rx.Observable<Task> traverse(JobId rootId, Func1<? super Task, Boolean> predicate) {
		return rx.Observable.<Task>create(subscriber -> {
				traverse(rootId, subscriber);
				subscriber.onCompleted();
		}).filter(predicate);
	}

	private void traverse(JobId rootId, Subscriber<? super Task> subscriber) {
		Task t = taskTable.get(rootId);
		if (t == null)
			subscriber.onError(new TaskRepositoryException("Couldn't find task[id=" + rootId + "]"));
		else {
			subscriber.onNext(t);
			ofNullable(childrenIndex.get(t.getId()))
				.ifPresent(children -> children.forEach(id -> traverse(id, subscriber)));
		}
	}

	@Override
	public rx.Observable<Task> traverse(JobId rootId, Task.Result result) {
		return traverse(rootId, task -> task.getResult().equals(result));
	}

	@Override
	public Stream<Task> traverseFailed() {
		return traverse()
			.filter(t -> t.getResult().equals(Task.Result.FAILED));
	}
}
