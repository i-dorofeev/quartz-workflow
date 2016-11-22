package ru.dorofeev.sandbox.quartzworkflow.jobs;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.JobKey;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions;
import ru.dorofeev.sandbox.quartzworkflow.serialization.Serializable;
import rx.Observable;

import java.util.Optional;

public interface JobRepository {

	rx.Observable<Event> bind(Observable<Cmd> input);

	Observable<Throwable> getErrors();

	Job addJob(JobId parentId, JobKey jobKey, Serializable args, QueueingOptions queueingOptions);

	Optional<Job> findJob(JobId jobId);

	rx.Observable<Job> traverseAll(Job.Result result);

	rx.Observable<Job> traverseSubTree(JobId rootId, Job.Result result);

	interface Cmd { }

	interface Event { }

	class JobAddedEvent implements Event {

		private final Job job;

		JobAddedEvent(Job job) {
			this.job = job;
		}

		public Job getJob() {
			return job;
		}
	}

	class JobCompletedEvent implements Event {

		private final JobId jobId;

		JobCompletedEvent(JobId jobId) {
			this.jobId = jobId;
		}

		public JobId getJobId() {
			return jobId;
		}
	}

	class AddJobCmd implements Cmd {

		private final JobId parentId;
		private final JobKey jobKey;
		private final Serializable args;
		private final QueueingOptions queueingOptions;

		AddJobCmd(JobId parentId, JobKey jobKey, Serializable args, QueueingOptions queueingOptions) {
			this.parentId = parentId;
			this.jobKey = jobKey;
			this.args = args;
			this.queueingOptions = queueingOptions;
		}

		JobId getParentId() {
			return parentId;
		}

		JobKey getJobKey() {
			return jobKey;
		}

		Serializable getArgs() {
			return args;
		}

		QueueingOptions getQueueingOptions() {
			return queueingOptions;
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

	static AddJobCmd addJobCmd(JobId parentId, JobKey jobKey, Serializable args, QueueingOptions queueingOptions) {
		return new AddJobCmd(parentId, jobKey, args, queueingOptions);
	}
}
