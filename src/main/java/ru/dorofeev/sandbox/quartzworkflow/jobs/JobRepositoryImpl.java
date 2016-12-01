package ru.dorofeev.sandbox.quartzworkflow.jobs;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.JobKey;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions;
import ru.dorofeev.sandbox.quartzworkflow.serialization.Serializable;
import ru.dorofeev.sandbox.quartzworkflow.utils.Clock;
import ru.dorofeev.sandbox.quartzworkflow.utils.ErrorObservable;
import rx.Observable;
import rx.subjects.PublishSubject;

import java.util.Optional;

import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.PARALLEL;
import static rx.schedulers.Schedulers.io;

class JobRepositoryImpl implements JobRepository {

	private final JobStore store;
	private final PublishSubject<Event> events = PublishSubject.create();
	private final ErrorObservable errors = new ErrorObservable();
	private final Clock clock;

	JobRepositoryImpl(JobStore store, Clock clock) {
		this.store = store;
		this.clock = clock;
	}

	@Override
	public rx.Observable<Event> bind(Observable<Cmd> input) {

		// we push output events asynchronously in order to
		// decrease time of getting a response by a client
		// (see .observeOn(io()) statements)
		input.ofType(AddJobCmd.class)
			.compose(errors.mapRetry(cmd -> addJobInternal(cmd.getParentId(), cmd.getJobKey(), cmd.getArgs(), cmd.getQueueingOptions())))
			.observeOn(io())
			.subscribe(this.events);

		input.ofType(CompleteJobCmd.class)
			.compose(errors.mapRetry(cmd -> {
				if (cmd.getException() != null)
					store.recordJobResult(cmd.getJobId(), Job.Result.FAILED, cmd.getException());
				else
					store.recordJobResult(cmd.getJobId(), Job.Result.SUCCESS, null);

				return new JobCompletedEvent(cmd.getJobId());
			}))
			.observeOn(io())
			.subscribe(events);

		return events;
	}

	@Override
	public Observable<Throwable> getErrors() {
		return errors.asObservable();
	}

	@Override
	public Job addJob(JobId parentId, JobKey jobKey, Serializable args, QueueingOptions queueingOptions) {
		JobAddedEvent event = addJobInternal(parentId, jobKey, args, queueingOptions);
		events.onNext(event);
		return event.getJob();
	}

	private JobAddedEvent addJobInternal(JobId parentId, JobKey jobKey, Serializable args, QueueingOptions queueingOptions) {

		String queueName = queueingOptions != null ? queueingOptions.getQueueName() : "default";
		QueueingOptions.ExecutionType executionType = queueingOptions != null ? queueingOptions.getExecutionType() : PARALLEL;

		Job job = store.saveNewJob(parentId, queueName, executionType, jobKey, args, clock.currentTime());
		return new JobAddedEvent(job);
	}

	@Override
	public Optional<Job> findJob(JobId jobId) {
		return store.findJob(jobId);
	}

	@Override
	public rx.Observable<Job> traverseAll(Job.Result result) {
		return store.traverseAll(result);
	}

	@Override
	public rx.Observable<Job> traverseSubTree(JobId rootId, Job.Result result) {
		return store.traverseSubTree(rootId, result);
	}
}
