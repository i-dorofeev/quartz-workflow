package ru.dorofeev.sandbox.quartzworkflow.tests.jobstore;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.JobKey;
import ru.dorofeev.sandbox.quartzworkflow.NodeSpecification;
import ru.dorofeev.sandbox.quartzworkflow.jobs.Job;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobStore;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions;
import ru.dorofeev.sandbox.quartzworkflow.tests.utils.HSqlDb;
import ru.dorofeev.sandbox.quartzworkflow.utils.RandomUUIDGenerator;
import ru.dorofeev.sandbox.quartzworkflow.utils.UUIDGenerator;

import java.util.ArrayDeque;
import java.util.Date;
import java.util.HashSet;
import java.util.Queue;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static ru.dorofeev.sandbox.quartzworkflow.jobs.JobStoreFactory.sqlJobStore;
import static ru.dorofeev.sandbox.quartzworkflow.serialization.SerializationFactory.jsonSerialization;

public class SqlJobStoreEnsureUniqueIdsTest {

	private static HSqlDb hsql;

	@BeforeClass
	public static void beforeClass() {
		hsql = new HSqlDb();
	}

	@AfterClass
	public static void afterClass() {
		hsql.shutdown();
	}

	@Test
	public void sanityTest() {

		TestUUIDGenerator uuidGenerator = new TestUUIDGenerator();

		JobStore jobStore = sqlJobStore(hsql.getDataSource(), uuidGenerator).spawn(jsonSerialization());

		uuidGenerator.pushUuid("00000000-0000-0000-0000-000000000001");
		Job job1 = jobStore.saveNewJob(null, "default", QueueingOptions.ExecutionType.PARALLEL, new JobKey("jobKey"), new StubArgs("value"), new Date(), NodeSpecification.ANY_NODE);
		assertThat(job1.getId(), is(equalTo(new JobId("00000000-0000-0000-0000-000000000001"))));

		uuidGenerator.pushUuid("00000000-0000-0000-0000-000000000001");
		uuidGenerator.pushUuid("00000000-0000-0000-0000-000000000002");
		Job job2 = jobStore.saveNewJob(null, "default", QueueingOptions.ExecutionType.PARALLEL, new JobKey("jobKey"), new StubArgs("value"), new Date(), NodeSpecification.ANY_NODE);
		assertThat(job2.getId(), is(equalTo(new JobId("00000000-0000-0000-0000-000000000002"))));
	}

	//@Test
	public void uuidUniquenessTest() {

		// my machine ran out of memory before the first duplicate was found
		// I don't know how many attempts were taken to get there

		HashSet<String> uuids = new HashSet<>();
		RandomUUIDGenerator randomUUIDGenerator = new RandomUUIDGenerator();

		long attempt = 1;
		while (true) {
			boolean isNew = uuids.add(randomUUIDGenerator.newUuid());
			assertThat("attempt #" + attempt , isNew, is(true));
			attempt++;
		}
	}

	private static class TestUUIDGenerator implements UUIDGenerator {

		private final Queue<String> uuidQueue = new ArrayDeque<>();

		void pushUuid(String uuid) {
			uuidQueue.offer(uuid);
		}

		@Override
		public String newUuid() {
			return uuidQueue.remove();
		}
	}
}
