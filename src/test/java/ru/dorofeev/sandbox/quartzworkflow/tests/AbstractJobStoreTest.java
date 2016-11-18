package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.FixMethodOrder;
import org.junit.Test;
import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.JobKey;
import ru.dorofeev.sandbox.quartzworkflow.jobs.Job;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobStore;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType;
import ru.dorofeev.sandbox.quartzworkflow.serialization.Serializable;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObject;
import rx.Observable;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;
import static ru.dorofeev.sandbox.quartzworkflow.jobs.JobStoreFactory.inMemoryJobStore;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.EXCLUSIVE;
import static ru.dorofeev.sandbox.quartzworkflow.serialization.SerializationFactory.jsonSerialization;

@FixMethodOrder(NAME_ASCENDING)
public class AbstractJobStoreTest {

	private static final JobStore store = inMemoryJobStore().call(jsonSerialization());

	private static JobId jobId;

	@Test
	public void test001_emptyStore() {

		Observable<Job> jobs = store.traverse(null, null);

		assertTrue(jobs.isEmpty().toBlocking().single());
	}

	@Test
	public void test002_addJob() {

		StubArgs args = new StubArgs("test");

		ExecutionType executionType = EXCLUSIVE;
		String queueName = "default";
		JobKey jobKey = new JobKey("jobKey");
		store.saveNewJob(null, queueName, executionType, jobKey, args);

		List<Job> jobs = store.traverse(null, null).toList().toBlocking().single();

		assertEquals(1, jobs.size());
		jobId = assertNewJob(jobs.get(0), queueName, executionType, jobKey, args);
	}

	@Test
	public void test003_findJobById() {

		Optional<Job> job = store.findJob(jobId);

		assertTrue(job.isPresent());
		assertEquals(jobId, job.get().getId());
	}

	private JobId assertNewJob(Job job, String queueName, ExecutionType executionType, JobKey jobKey, StubArgs args) {

		assertNotNull(job.getId());
		assertNull(job.getResult());
		assertEquals(args, StubArgs.deserializeFrom(job.getArgs()));
		assertNull(job.getException());
		assertEquals(executionType, job.getExecutionType());
		assertEquals(jobKey, job.getJobKey());
		assertEquals(queueName, job.getQueueName());

		return job.getId();
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
