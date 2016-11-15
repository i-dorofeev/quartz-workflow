package ru.dorofeev.sandbox.quartzworkflow.taskrepo;

import ru.dorofeev.sandbox.quartzworkflow.JobKey;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOption;
import ru.dorofeev.sandbox.quartzworkflow.TaskId;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObject;
import rx.Observable;
import rx.functions.Func1;

import java.util.Optional;
import java.util.stream.Stream;

import static ru.dorofeev.sandbox.quartzworkflow.taskrepo.TaskRepository.EventType.ADD;
import static ru.dorofeev.sandbox.quartzworkflow.taskrepo.TaskRepository.EventType.COMPLETE;

public interface TaskRepository {

	rx.Observable<Event> bind(Observable<Cmd> input);

	Observable<Throwable> getErrors();

	Task addTask(TaskId parentId, JobKey jobKey, SerializedObject args, QueueingOption queueingOption);

	Optional<Task> findTask(TaskId taskId);

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

		Event(EventType eventType, Task task) {
			this.eventType = eventType;
			this.task = task;
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
		private final SerializedObject args;
		private final QueueingOption queueingOption;

		AddTaskCmd(TaskId parentId, JobKey jobKey, SerializedObject args, QueueingOption queueingOption) {
			this.parentId = parentId;
			this.jobKey = jobKey;
			this.args = args;
			this.queueingOption = queueingOption;
		}

		TaskId getParentId() {
			return parentId;
		}

		JobKey getJobKey() {
			return jobKey;
		}

		SerializedObject getArgs() {
			return args;
		}

		QueueingOption getQueueingOption() {
			return queueingOption;
		}
	}

	class CompleteTaskCmd implements Cmd {
		private final TaskId taskId;
		private final Throwable exception;

		CompleteTaskCmd(TaskId taskId, Throwable exception) {
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

	static CompleteTaskCmd completeTaskCmd(TaskId taskId, Throwable ex) {
		return new CompleteTaskCmd(taskId, ex);
	}

	static AddTaskCmd addTaskCmd(TaskId parentId, JobKey jobKey, SerializedObject args, QueueingOption queueingOption) {
		return new AddTaskCmd(parentId, jobKey, args, queueingOption);
	}
}
