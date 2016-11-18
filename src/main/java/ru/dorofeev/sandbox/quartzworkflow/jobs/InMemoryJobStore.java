package ru.dorofeev.sandbox.quartzworkflow.jobs;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.JobKey;
import ru.dorofeev.sandbox.quartzworkflow.jobs.Job.Result;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType;
import ru.dorofeev.sandbox.quartzworkflow.serialization.Serializable;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObject;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObjectFactory;
import rx.*;
import rx.functions.Func1;

import java.util.*;

import static java.lang.Enum.valueOf;
import static java.util.Optional.ofNullable;

class InMemoryJobStore implements JobStore {

	private final SerializedObjectFactory serializedObjectFactory;

	private final Map<String, InMemoryJobRecord> jobTable = new HashMap<>();
	private final Map<String, Set<String>> childrenIndex = new HashMap<>();
	private final Object sync = new Object();

	InMemoryJobStore(SerializedObjectFactory serializedObjectFactory) {
		this.serializedObjectFactory = serializedObjectFactory;
	}

	private Optional<InMemoryJobRecord> getById(JobId jobId) {
		return ofNullable(jobTable.get(jobId.toString()));
	}

	@Override
	public Optional<Job> findJob(JobId jobId) {
		synchronized (sync) {
			return getById(jobId).map(this::toJob);
		}
	}

	@Override
	public void recordJobResult(JobId jobId, Result result, Throwable ex) {
		synchronized (sync) {
			InMemoryJobRecord job = getById(jobId)
				.orElseThrow(() -> new JobRepositoryException("Couldn't find job[id=" + jobId + "]"));

			job.setResult(result.toString());

			if (ex != null)
				job.setException( ex.toString());
		}
	}

	@Override
	public Job saveNewJob(JobId parentId, String queueName, ExecutionType executionType, JobKey jobKey, Serializable args) {
		synchronized (sync) {

			if (parentId != null && !jobTable.containsKey(parentId.toString()))
				throw new JobRepositoryException("Job[id=" + parentId + "] not found");

			String jobId = nextJobId();

			SerializedObject serializedArgs = serializedObjectFactory.spawn();
			args.serializeTo(serializedArgs);

			InMemoryJobRecord job = new InMemoryJobRecord();
			job.setJobId(jobId);
			job.setResult(null);
			job.setException(null);
			job.setExecutionType(executionType.toString());
			job.setJobKey(jobKey.toString());
			job.setQueueName(queueName);
			job.setSerializedArgs(serializedArgs.build());

			jobTable.put(jobId, job);

			if (parentId != null)
				indexChild(parentId.toString(), jobId);

			return toJob(job);
		}
	}

	private Job toJob(InMemoryJobRecord record) {
		JobId id = new JobId(record.getJobId());
		String queueName = record.getQueueName();
		ExecutionType executionType = ofNullable(record.getExecutionType()).map(et -> valueOf(ExecutionType.class, et)).orElse(null);
		Result result = ofNullable(record.getResult()).map(r -> valueOf(Result.class, r)).orElse(null);
		String exception = record.getException();
		JobKey jobKey = new JobKey(record.getJobKey());
		SerializedObject args = serializedObjectFactory.spawn(record.getSerializedArgs());

		return new Job(id, queueName, executionType, result, exception, jobKey, args);
	}

	private rx.Observable<Job> traverse(JobId rootId, Func1<? super Job, Boolean> predicate) {
		if (rootId != null) {
			return rx.Observable.<Job>create(subscriber -> {
				traverseByRoot(rootId.toString(), subscriber);
				subscriber.onCompleted();
			}).filter(predicate);
		} else {
			return traverseAll(predicate);
		}
	}

	private rx.Observable<Job> traverseAll(Func1<? super Job, Boolean> predicate) {
		return rx.Observable.from(jobTable.values())
			.map(this::toJob)
			.filter(predicate);
	}

	private void traverseByRoot(String rootId, Subscriber<? super Job> subscriber) {
		InMemoryJobRecord record = jobTable.get(rootId);
		if (record == null)
			subscriber.onError(new JobRepositoryException("Couldn't find job[id=" + rootId + "]"));
		else {
			subscriber.onNext(toJob(record));
			ofNullable(childrenIndex.get(record.getJobId()))
				.ifPresent(children -> children.forEach(id -> traverseByRoot(id, subscriber)));
		}
	}

	@Override
	public rx.Observable<Job> traverse(JobId rootId, Result result) {
		synchronized (sync) {

			Func1<? super Job, Boolean> jobFilter = result != null ? (Job job) -> result.equals(job.getResult()) : (Job job) -> true;
			return traverse(rootId, jobFilter);
		}
	}

	private void indexChild(String parent, String child) {
		Set<String> children = childrenIndex.get(parent);
		if (children == null) {
			children = new HashSet<>();
			childrenIndex.put(parent, children);
		}

		children.add(child);
	}

	private String nextJobId() {
		String jobId = UUID.randomUUID().toString();
		while (jobTable.containsKey(jobId)) {
			jobId = UUID.randomUUID().toString();
		}
		return jobId;
	}
}
