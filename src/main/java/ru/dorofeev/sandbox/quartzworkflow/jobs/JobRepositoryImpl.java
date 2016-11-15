package ru.dorofeev.sandbox.quartzworkflow.jobs;

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

class JobRepositoryImpl implements JobRepository {

	private final Map<JobId, Job> jobTable = new HashMap<>();
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

		input.ofType(AddJobCmd.class)
			.compose(errors.mapRetry(cmd -> addJobInternal(cmd.getParentId(), cmd.getJobKey(), cmd.getArgs(), cmd.getQueueingOption())))
			.subscribe(this.events);

		input.ofType(CompleteJobCmd.class)
			.compose(errors.mapRetry(cmd -> {
				Job job = ofNullable(jobTable.get(cmd.getJobId()))
					.orElseThrow(() -> new JobRepositoryException("Couldn't find job[id=" + cmd.getJobId() + "]"));

				if (cmd.getException() != null)
					job.recordResult(Job.Result.FAILED, cmd.getException());
				else
					job.recordResult(Job.Result.SUCCESS, null);

				return new Event(EventType.COMPLETE, job);
			}))
			.subscribe(events);

		return events;
	}

	@Override
	public Observable<Throwable> getErrors() {
		return errors.asObservable();
	}

	private JobId nextJobId() {
		JobId jobId = JobId.createUniqueJobId();
		while (jobTable.containsKey(jobId)) {
			jobId = JobId.createUniqueJobId();
		}
		return jobId;
	}

	@Override
	public Job addJob(JobId parentId, JobKey jobKey, SerializedObject args, QueueingOption queueingOption) {
		Event event = addJobInternal(parentId, jobKey, args, queueingOption);
		events.onNext(event);
		return event.getJob();
	}

	private synchronized Event addJobInternal(JobId parentId, JobKey jobKey, SerializedObject args, QueueingOption queueingOption) {
		if (parentId != null && !jobTable.containsKey(parentId))
			throw new JobRepositoryException("Job[id=" + parentId + "] not found");

		JobId jobId = nextJobId();
		String queueName = queueingOption != null ? queueingOption.getQueueName() : "default";
		QueueingOption.ExecutionType executionType = queueingOption != null ? queueingOption.getExecutionType() : PARALLEL;

		Job t = new Job(jobId, queueName, executionType, jobKey, args);
		jobTable.put(jobId, t);

		if (parentId != null)
			indexChild(parentId, t.getId());

		return new Event(EventType.ADD, t);
	}

	@Override
	public Optional<Job> findJob(JobId jobId) {
		return ofNullable(jobTable.get(jobId));
	}

	@Override
	public Stream<Job> traverse() {
		return jobTable.values().stream();
	}

	@Override
	public rx.Observable<Job> traverse(Job.Result result) {
		return rx.Observable.<Job>create(s -> {
			jobTable.values().forEach(s::onNext);
			s.onCompleted();
		}).filter(t -> t.getResult() == result);
	}

	@Override
	public rx.Observable<Job> traverse(JobId rootId, Func1<? super Job, Boolean> predicate) {
		return rx.Observable.<Job>create(subscriber -> {
				traverse(rootId, subscriber);
				subscriber.onCompleted();
		}).filter(predicate);
	}

	private void traverse(JobId rootId, Subscriber<? super Job> subscriber) {
		Job t = jobTable.get(rootId);
		if (t == null)
			subscriber.onError(new JobRepositoryException("Couldn't find job[id=" + rootId + "]"));
		else {
			subscriber.onNext(t);
			ofNullable(childrenIndex.get(t.getId()))
				.ifPresent(children -> children.forEach(id -> traverse(id, subscriber)));
		}
	}

	@Override
	public rx.Observable<Job> traverse(JobId rootId, Job.Result result) {
		return traverse(rootId, job -> job.getResult().equals(result));
	}

	@Override
	public Stream<Job> traverseFailed() {
		return traverse()
			.filter(t -> t.getResult().equals(Job.Result.FAILED));
	}
}
