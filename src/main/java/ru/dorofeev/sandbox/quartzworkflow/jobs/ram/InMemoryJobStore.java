package ru.dorofeev.sandbox.quartzworkflow.jobs.ram;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.JobKey;
import ru.dorofeev.sandbox.quartzworkflow.NodeId;
import ru.dorofeev.sandbox.quartzworkflow.NodeSpecification;
import ru.dorofeev.sandbox.quartzworkflow.jobs.Job;
import ru.dorofeev.sandbox.quartzworkflow.jobs.Job.Result;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobRepositoryException;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobStore;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType;
import ru.dorofeev.sandbox.quartzworkflow.serialization.Serializable;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObject;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObjectFactory;
import ru.dorofeev.sandbox.quartzworkflow.utils.RandomUUIDGenerator;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

import java.util.*;

import static java.lang.Enum.valueOf;
import static java.util.Optional.ofNullable;
import static ru.dorofeev.sandbox.quartzworkflow.jobs.Job.Result.CREATED;
import static ru.dorofeev.sandbox.quartzworkflow.utils.Contracts.shouldNotBeNull;

public class InMemoryJobStore implements JobStore {

	private final SerializedObjectFactory serializedObjectFactory;

	private final Map<String, InMemoryJobRecord> jobTable = new HashMap<>();
	private final Map<String, Set<String>> childrenIndex = new HashMap<>();
	private final Object sync = new Object();

	private final RandomUUIDGenerator uuidGenerator = new RandomUUIDGenerator();

	public InMemoryJobStore(SerializedObjectFactory serializedObjectFactory) {
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
	public void recordJobResult(JobId jobId, Result result, Throwable ex, long executionDuration, Date completed, NodeId completedNodeId) {
		synchronized (sync) {

			shouldNotBeNull(completedNodeId, "completedNodeId should be specified");

			InMemoryJobRecord job = getById(jobId)
				.orElseThrow(() -> new JobRepositoryException("Couldn't find job[id=" + jobId + "]"));

			job.setResult(result.toString());
			job.setExecutionDuration(executionDuration);
			job.setCompleted(completed);
			job.setCompletedNodeId(completedNodeId.value());

			if (ex != null)
				job.setException(ex.toString());
			else
				job.setException(null);
		}
	}

	@Override
	public Job saveNewJob(JobId parentId, String queueName, ExecutionType executionType, JobKey jobKey, Serializable args, Date created, NodeSpecification targetNodeSpecification) {
		synchronized (sync) {

			shouldNotBeNull(targetNodeSpecification, "Target node specification should not be null.");

			if (parentId != null && !jobTable.containsKey(parentId.toString()))
				throw new JobRepositoryException("Job[id=" + parentId + "] not found");

			String jobId = nextJobId();

			SerializedObject serializedArgs = serializedObjectFactory.spawn();
			args.serializeTo(serializedArgs);

			InMemoryJobRecord job = new InMemoryJobRecord();
			job.setJobId(jobId);
			job.setParentId(parentId != null ? parentId.toString() : null);
			job.setResult(CREATED.toString());
			job.setException(null);
			job.setExecutionType(executionType.toString());
			job.setJobKey(jobKey.toString());
			job.setQueueName(queueName);
			job.setSerializedArgs(serializedArgs.build());
			job.setCreated(created);
			job.setTargetNodeSpecification(targetNodeSpecification.asString());

			jobTable.put(jobId, job);

			if (parentId != null)
				indexChild(parentId.toString(), jobId);

			return toJob(job);
		}
	}

	private Job toJob(InMemoryJobRecord record) {
		JobId id = new JobId(record.getJobId());
		JobId parentId = record.getParentId() != null ? new JobId(record.getParentId()) : null;
		String queueName = record.getQueueName();
		ExecutionType executionType = ofNullable(record.getExecutionType()).map(et -> valueOf(ExecutionType.class, et)).orElse(null);
		Result result = ofNullable(record.getResult()).map(r -> valueOf(Result.class, r)).orElse(null);
		String exception = record.getException();
		JobKey jobKey = new JobKey(record.getJobKey());
		SerializedObject args = serializedObjectFactory.spawn(record.getSerializedArgs());
		Date created = record.getCreated();
		NodeSpecification targetNodeSpecification = NodeSpecification.fromString(record.getTargetNodeSpecification());
		Long executionDuration = record.getExecutionDuration();
		Date completed = record.getCompleted();
		NodeId completedNodeId = NodeId.fromString(record.getCompletedNodeId());

		return new Job(id, parentId, queueName, executionType, result, exception, jobKey, args, created, targetNodeSpecification, executionDuration, completed, completedNodeId);
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
	public rx.Observable<Job> traverseSubTree(JobId rootId, Result result) {
		synchronized (sync) {
			if (rootId == null)
				throw new IllegalArgumentException("rootId must be non null");

			Func1<? super Job, Boolean> jobFilter = result != null ? (Job job) -> result.equals(job.getResult()) : (Job job) -> true;

			return rx.Observable.<Job>create(subscriber -> {
				traverseByRoot(rootId.toString(), subscriber);
				subscriber.onCompleted();
			}).filter(jobFilter);
		}
	}

	@Override
	public rx.Observable<Job> traverseAll(Result result) {
		synchronized (sync) {
			Func1<? super Job, Boolean> jobFilter = result != null ? (Job job) -> result.equals(job.getResult()) : (Job job) -> true;

			return rx.Observable.from(jobTable.values())
				.map(this::toJob)
				.filter(jobFilter);
		}
	}

	@Override
	public Observable<Job> traverseRoots() {
		return traverseAll(null)
			.filter(j -> !j.getParentId().isPresent());
	}

	private void indexChild(String parent, String child) {
		childrenIndex.computeIfAbsent(parent, k -> new HashSet<>()).add(child);
	}

	private String nextJobId() {
		String jobId = uuidGenerator.newUuid();
		while (jobTable.containsKey(jobId)) {
			jobId = uuidGenerator.newUuid();
		}
		return jobId;
	}
}
