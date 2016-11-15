package ru.dorofeev.sandbox.quartzworkflow.jobs;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.JobKey;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOption;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObject;
import rx.Observable;
import rx.functions.Func1;

import java.util.Optional;
import java.util.stream.Stream;

import static ru.dorofeev.sandbox.quartzworkflow.jobs.JobRepository.EventType.ADD;
import static ru.dorofeev.sandbox.quartzworkflow.jobs.JobRepository.EventType.COMPLETE;

public interface JobRepository {

	rx.Observable<Event> bind(Observable<Cmd> input);

	Observable<Throwable> getErrors();

	Job addJob(JobId parentId, JobKey jobKey, SerializedObject args, QueueingOption queueingOption);

	Optional<Job> findJob(JobId jobId);

	Stream<Job> traverse();

	rx.Observable<Job> traverse(Job.Result result);

	rx.Observable<Job> traverse(JobId rootId, Func1<? super Job, Boolean> predicate);

	rx.Observable<Job> traverse(JobId rootId, Job.Result result);

	Stream<Job> traverseFailed();

	enum EventType { ADD, COMPLETE }

	interface Cmd { }

	class Event {

		private final EventType eventType;
		private final Job job;

		Event(EventType eventType, Job job) {
			this.eventType = eventType;
			this.job = job;
		}

		public Job getJob() {
			return job;
		}

		public boolean isAdd() {
			return this.eventType.equals(ADD);
		}

		public boolean isComplete() {
			return this.eventType.equals(COMPLETE);
		}
	}

	class AddJobCmd implements Cmd {

		private final JobId parentId;
		private final JobKey jobKey;
		private final SerializedObject args;
		private final QueueingOption queueingOption;

		AddJobCmd(JobId parentId, JobKey jobKey, SerializedObject args, QueueingOption queueingOption) {
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

	class CompleteJobCmd implements Cmd {
		private final JobId jobId;
		private final Throwable exception;

		CompleteJobCmd(JobId jobId, Throwable exception) {
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

	static CompleteJobCmd completeJobCmd(JobId jobId, Throwable ex) {
		return new CompleteJobCmd(jobId, ex);
	}

	static AddJobCmd addJobCmd(JobId parentId, JobKey jobKey, SerializedObject args, QueueingOption queueingOption) {
		return new AddJobCmd(parentId, jobKey, args, queueingOption);
	}
}
