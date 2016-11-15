package ru.dorofeev.sandbox.quartzworkflow;

import rx.functions.Func1;

import java.util.stream.Stream;

import static ru.dorofeev.sandbox.quartzworkflow.TaskRepository.EventType.ADD;
import static ru.dorofeev.sandbox.quartzworkflow.TaskRepository.EventType.COMPLETE;

public interface TaskRepository {

	Stream<Task> traverse();

	rx.Observable<Task> traverse(Task.Result result);

	rx.Observable<Task> traverse(TaskId rootId, Func1<? super Task, Boolean> predicate);

	rx.Observable<Task> traverse(TaskId rootId, Task.Result result);

	Stream<Task> traverseFailed();

	enum EventType { ADD, COMPLETE }

	interface Cmd { }

	class Event {

		private final EventType eventType;
		private final Task task;

		public Event(EventType eventType, Task task) {
			this.eventType = eventType;
			this.task = task;
		}

		public EventType getEventType() {
			return eventType;
		}

		public Task getTask() {
			return task;
		}

		public boolean isAdd() {
			return this.eventType.equals(ADD);
		}

		public boolean isComplete() {
			return this.eventType.equals(COMPLETE);
		}
	}

	class AddTaskCmd implements Cmd {

		private final TaskId parentId;
		private final JobKey jobKey;
		private final JobDataMap jobDataMap;
		private final QueueingOption queueingOption;

		public AddTaskCmd(TaskId parentId, JobKey jobKey, JobDataMap jobDataMap, QueueingOption queueingOption) {
			this.parentId = parentId;
			this.jobKey = jobKey;
			this.jobDataMap = jobDataMap;
			this.queueingOption = queueingOption;
		}

		public TaskId getParentId() {
			return parentId;
		}

		public JobKey getJobKey() {
			return jobKey;
		}

		public JobDataMap getJobDataMap() {
			return jobDataMap;
		}

		public QueueingOption getQueueingOption() {
			return queueingOption;
		}
	}

	class CompleteTaskCmd implements Cmd {
		private final TaskId taskId;
		private final Throwable exception;

		public CompleteTaskCmd(TaskId taskId, Throwable exception) {
			this.taskId = taskId;
			this.exception = exception;
		}

		public TaskId getTaskId() {
			return taskId;
		}

		public Throwable getException() {
			return exception;
		}
	}
}
