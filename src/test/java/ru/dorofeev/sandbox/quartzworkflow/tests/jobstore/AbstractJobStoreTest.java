package ru.dorofeev.sandbox.quartzworkflow.tests.jobstore;

import org.junit.FixMethodOrder;
import org.junit.Test;
import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.JobKey;
import ru.dorofeev.sandbox.quartzworkflow.jobs.Job;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobStore;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType;
import ru.dorofeev.sandbox.quartzworkflow.serialization.Serializable;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObject;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObjectFactory;
import rx.Observable;

import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;
import static ru.dorofeev.sandbox.quartzworkflow.jobs.Job.Result.*;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.EXCLUSIVE;
import static rx.Observable.*;

@FixMethodOrder(NAME_ASCENDING)
public abstract class AbstractJobStoreTest {

	private static final JobKey jobKey = new JobKey("jobKey");
	private static final StubArgs args = new StubArgs("test");

	private static JobId jobId;
	private static List<JobId> childJobs;
	private static Date created;

	protected abstract JobStore getStore();
	protected abstract SerializedObjectFactory getSerializedObjectFactory();

	@Test
	public void test010_emptyStore() {

		Observable<Job> jobs = getStore().traverseAll(null);

		assertTrue(jobs.isEmpty().toBlocking().single());
	}

	@Test
	public void test020_addJob() {

		created = new Date();
		Job newJob = getStore().saveNewJob(/* parentId */ null, "default", EXCLUSIVE, jobKey, args, created);

		jobId = newJob.getId();

		assertJobsEqual(new Job(jobId, /* parentId */ null, "default", EXCLUSIVE, CREATED, /* exception */ null, jobKey, serializedArgs(args), created, null, null), newJob);

		assertFindById(newJob.getId(), newJob);

		assertTraverseSingle(null, newJob);
		assertTraverseSingle(CREATED, newJob);
		assertTraverseNone(SUCCESS);
		assertTraverseNone(FAILED);
	}

	@Test
	public void test030_recordFailed() {

		long executionDuration = 1000L;
		Date completed = new Date();
		getStore().recordJobResult(jobId, FAILED, new RuntimeException("stub exception"), executionDuration, completed);

		Job job = assertFindById(jobId, new Job(jobId, /* parentId */ null, "default", EXCLUSIVE, FAILED, "java.lang.RuntimeException: stub exception", jobKey, serializedArgs(args), created, executionDuration, completed));

		assertTraverseSingle(null, job);
		assertTraverseNone(CREATED);
		assertTraverseNone(SUCCESS);
		assertTraverseSingle(FAILED, job);
	}

	@Test
	public void test040_recordSuccess() {

		long executionDuration = 1000L;
		Date completed = new Date();
		getStore().recordJobResult(jobId, SUCCESS, null, executionDuration, completed);

		Job job = assertFindById(jobId, new Job(jobId, /* parentId */ null, "default", EXCLUSIVE, SUCCESS, /* exception */ null, jobKey, serializedArgs(args), created, executionDuration, completed));

		assertTraverseSingle(null, job);
		assertTraverseNone(CREATED);
		assertTraverseSingle(SUCCESS, job);
		assertTraverseNone(FAILED);
	}

	@Test
	public void test060_traverse() {

		// create subtree jobs
		childJobs = range(1, 10)
			.flatMap(i -> saveJobHierarchy(jobId, 3, "default", EXCLUSIVE, jobKey, args))
			.toList().toBlocking().single();

		// create root trees
		range(1, 10)
			.flatMap(i -> saveJobHierarchy(null, 3, "default", EXCLUSIVE, jobKey, args))
			.toList().toBlocking().single();

		Observable<JobId> actualChildJobs = getStore().traverseSubTree(jobId, null)
			.map(Job::getId);

		assertThat(actualChildJobs.filter(i -> !jobId.equals(i)).toList().toBlocking().single(), containsInAnyOrder(childJobs.toArray()));
		assertThat(getStore().traverseRoots().count().toBlocking().single(), equalTo(11));
	}

	@Test
	public void test070_traverseByRootAndResult() {
		getStore().recordJobResult(childJobs.get(13), FAILED, new RuntimeException("stub"), 100L, new Date()); // 13 is some magic number between 0 and 30

		Observable<JobId> failed = getStore().traverseSubTree(jobId, FAILED)
			.map(Job::getId);

		assertThat(failed.toBlocking().single(), equalTo(childJobs.get(13)));

	}

	private Observable<JobId> saveJobHierarchy(JobId root, int depth, String queueName, ExecutionType executionType, JobKey jobKey, Serializable args) {
		if (depth == 0)
			return Observable.empty();

		Job job = getStore().saveNewJob(root, queueName, executionType, jobKey, args, new Date());
		return merge(
			just(job.getId()),
			saveJobHierarchy(job.getId(), depth - 1, queueName, executionType, jobKey, args));
	}

	private SerializedObject serializedArgs(Serializable args) {
		SerializedObject serializedArgs = getSerializedObjectFactory().spawn();
		args.serializeTo(serializedArgs);
		return serializedArgs;
	}

	private void assertJobsEqual(Job job1, Job job2) {
		assertEquals(job1.getId(), job2.getId());
		assertEquals(job1.getJobKey(), job2.getJobKey());
		assertEquals(job1.getQueueName(), job2.getQueueName());
		assertEquals(job1.getArgs(), job2.getArgs());
		assertEquals(job1.getExecutionType(), job2.getExecutionType());
		assertThat(job2.getException().orElse(""), containsString(job1.getException().orElse("")));
		assertEquals(job1.getResult(), job2.getResult());
		assertEquals(job1.getCreated(), job2.getCreated());
		assertEquals(job1.getExecutionDuration(), job2.getExecutionDuration());
		assertEquals(job1.getCompleted(), job2.getCompleted());
	}

	private void assertTraverseSingle(Job.Result result, Job expectedJob) {
		Job job = getStore().traverseAll(result).toBlocking().single();
		assertJobsEqual(expectedJob, job);
	}

	private void assertTraverseNone(Job.Result result) {
		List<Job> jobs = getStore().traverseAll(result).toList().toBlocking().single();

		if (jobs.size() != 0)
			throw new AssertionError("Expected no jobs but found " + jobs);
	}

	@SuppressWarnings("OptionalGetWithoutIsPresent")
	private Job assertFindById(JobId jobId, Job expectedJob) {
		Optional<Job> job = getStore().findJob(jobId);

		assertTrue(job.isPresent());
		assertJobsEqual(expectedJob, job.get());

		return job.get();
	}

}
