package ru.dorofeev.sandbox.quartzworkflow.taskrepo;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.JobKey;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOption;
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

	Task addTask(JobId parentId, JobKey jobKey, SerializedObject args, QueueingOption queueingOption);

	Optional<Task> findTask(JobId jobId);

	Stream<Task> traverse();

	rx.Observable<Task> traverse(Task.Result result);

	rx.Observable<Task> traverse(JobId rootId, Func1<? super Task, Boolean> predicate);

	rx.Observable<Task> traverse(JobId rootId, Task.Result result);

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

		private final JobId parentId;
		private final JobKey jobKey;
		private final SerializedObject args;
		private final QueueingOption queueingOption;

		AddTaskCmd(JobId parentId, JobKey jobKey, SerializedObject args, QueueingOption queueingOption) {
			this.parentId = parentId;
			this.jobKey = jobKey;
			this.args = args;
			this.queueingOption = queueingOption;
		}

		JobId getParentId() {
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
		private final JobId jobId;
		private final Throwable exception;

		CompleteTaskCmd(JobId jobId, Throwable exception) {
			this.jobId = jobId;
			this.exception = exception;
		}

		public JobId getJobId() {
			return jobId;
		}

		public Throwable getException() {
			return exception;
		}
	}

	static CompleteTaskCmd completeTaskCmd(JobId jobId, Throwable ex) {
		return new CompleteTaskCmd(jobId, ex);
	}

	static AddTaskCmd addTaskCmd(JobId parentId, JobKey jobKey, SerializedObject args, QueueingOption queueingOption) {
		return new AddTaskCmd(parentId, jobKey, args, queueingOption);
	}
}
