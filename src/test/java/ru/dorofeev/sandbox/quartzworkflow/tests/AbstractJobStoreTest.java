package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.FixMethodOrder;
import org.junit.Test;
import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.JobKey;
import ru.dorofeev.sandbox.quartzworkflow.jobs.Job;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobStore;
import ru.dorofeev.sandbox.quartzworkflow.serialization.Serializable;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObject;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObjectFactory;
import rx.Observable;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;
import static ru.dorofeev.sandbox.quartzworkflow.jobs.Job.Result.*;
import static ru.dorofeev.sandbox.quartzworkflow.jobs.JobStoreFactory.inMemoryJobStore;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.EXCLUSIVE;
import static ru.dorofeev.sandbox.quartzworkflow.serialization.SerializationFactory.jsonSerialization;

@FixMethodOrder(NAME_ASCENDING)
public class AbstractJobStoreTest {

	private static final SerializedObjectFactory serialization = jsonSerialization();

	private static final JobStore store = inMemoryJobStore().call(serialization);

	private static JobId jobId;

	@Test
	public void test001_emptyStore() {

		Observable<Job> jobs = store.traverse(null, null);

		assertTrue(jobs.isEmpty().toBlocking().single());
	}

	@Test
	public void test002_addJob() {

		StubArgs args = new StubArgs("test");

		JobKey jobKey = new JobKey("jobKey");
		store.saveNewJob(null, "default", EXCLUSIVE, jobKey, args);

		Job expectedJob = new Job(new AnyNonNullJobId(), "default", EXCLUSIVE, CREATED, /* exception */ null, jobKey, serializedArgs(args));

		JobId jobId = assertTraverseSingle(null, null, expectedJob);

		assertTraverseSingle(null, CREATED, expectedJob);
		assertTraverseNone(null, SUCCESS);
		assertTraverseNone(null, FAILED);
		assertTraverseNone(null, RUNNING);

		assertFindById(jobId, expectedJob);
	}

	private SerializedObject serializedArgs(Serializable args) {
		SerializedObject serializedArgs = serialization.spawn();
		args.serializeTo(serializedArgs);
		return serializedArgs;
	}

	private void assertJobsEqual(Job job1, Job job2) {
		assertEquals(job1.getId(), job2.getId());
		assertEquals(job1.getJobKey(), job2.getJobKey());
		assertEquals(job1.getQueueName(), job2.getQueueName());
		assertEquals(job1.getArgs(), job2.getArgs());
		assertEquals(job1.getExecutionType(), job2.getExecutionType());
		assertEquals(job1.getException(), job2.getException());
		assertEquals(job1.getResult(), job2.getResult());
	}

	private JobId assertTraverseSingle(JobId rootId, Job.Result result, Job expectedJob) {
		Job job = store.traverse(rootId, result).toBlocking().single();
		assertJobsEqual(expectedJob, job);
		return job.getId();
	}

	private void assertTraverseNone(JobId rootId, Job.Result result) {
		List<Job> jobs = store.traverse(rootId, result).toList().toBlocking().single();

		if (jobs.size() != 0)
			throw new AssertionError("Expected no jobs but found " + jobs);
	}

	private void assertFindById(JobId jobId, Job expectedJob) {
		Optional<Job> job = store.findJob(jobId);

		assertTrue(job.isPresent());
		assertJobsEqual(expectedJob, job.get());
	}

	private static class AnyNonNullJobId extends JobId {

		AnyNonNullJobId() {
			super(null);
		}

		@SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
		@Override
		public boolean equals(Object o) {
			return o != null;
		}

		@Override
		public int hashCode() {
			return 0;
		}
	}

	private static class StubArgs implements Serializable {

		private final String value;

		private StubArgs(String value) {
			this.value = value;
		}

		@Override
		public void serializeTo(SerializedObject serializedObject) {
			serializedObject.addString("value", value);
		}

		static StubArgs deserializeFrom(SerializedObject serializedObject) {
			String value = serializedObject.getString("value");
			return new StubArgs(value);
		}

		@Override
		public String toString() {
			return "StubArgs{" +
				"value='" + value + '\'' +
				'}';
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			StubArgs stubArgs = (StubArgs) o;

			return value != null ? value.equals(stubArgs.value) : stubArgs.value == null;

		}

		@Override
		public int hashCode() {
			return value != null ? value.hashCode() : 0;
		}
	}
}
