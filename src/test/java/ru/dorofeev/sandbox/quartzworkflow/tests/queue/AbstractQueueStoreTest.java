package ru.dorofeev.sandbox.quartzworkflow.tests.queue;

import org.hamcrest.Matcher;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueItem;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueStore;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueStoreException;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions;
import ru.dorofeev.sandbox.quartzworkflow.utils.UUIDGenerator;

import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.EXCLUSIVE;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.PARALLEL;

@FixMethodOrder(NAME_ASCENDING)
public abstract class AbstractQueueStoreTest {

	private static final QueueTracker queueTracker = new QueueTracker();
	private static final UUIDGenerator uuidGenerator = new SimpleUUIDGenerator();

	@Rule
	public final ExpectedException expectedException = ExpectedException.none();

	protected abstract QueueStore queueStore();

	@Test
	public void test050_initialData() {
		insertQueueItem("q1", PARALLEL);	// 1
		insertQueueItem("q1", PARALLEL);	// 2
		insertQueueItem("q2", PARALLEL);	// 3
		insertQueueItem("q2", PARALLEL);	// 4
		insertQueueItem("q1", EXCLUSIVE);	// 5
		insertQueueItem("q2", EXCLUSIVE);	// 6

		insertQueueItem("q3", PARALLEL);	// 7
		insertQueueItem("q3", PARALLEL);	// 8
		insertQueueItem("q4", PARALLEL);	// 9
		insertQueueItem("q4", PARALLEL);	// 10
		insertQueueItem("q3", EXCLUSIVE);	// 11
		insertQueueItem("q4", EXCLUSIVE);	// 12
	}

	@Test
	public void test060_cannotEnqueueSameJobTwice() {
		expectedException.expect(QueueStoreException.class);
		expectedException.expectMessage(containsString("is already enqueued"));

		queueStore().insertQueueItem(queueTracker.lastAdded().getJobId(), "q9", PARALLEL);
	}

	@Test
	public void test070_popParallel() {

		popNext("q1", asList(1L, 2L));
		popNext("q2", asList(3L, 4L));
	}

	@Test
	public void test080_releasePoppedAndPopExclusive() {
		release(1L, 2L);
		popNext("q1", singletonList(5L));

		release(3L, 4L);
		popNext("q2", singletonList(6L));
	}

	@Test
	public void test090_popParallelAllQueues() {
		release(5L, 6L);
		popNext(null, asList(7L, 8L, 9L, 10L));
	}

	@Test
	public void test100_releasePoppedAndPopExclusive_allQueues() {
		release(7L, 8L, 9L, 10L);
		popNext(null, asList(11L, 12L));
	}

	private void insertQueueItem(String queueName, QueueingOptions.ExecutionType executionType) {
		queueTracker.add(queueStore().insertQueueItem(newJobId(), queueName, executionType));
	}

	private JobId newJobId() {
		return new JobId(uuidGenerator.newUuid());
	}

	private void popNext(String queueName, List<Long> expectedItems) {
		System.out.println("Getting next " + expectedItems.size() + " queue items...");

		for (Long expectedItem : expectedItems)
			popNext(queueName, is(equalTo(of(expectedItem))));

		popNext(queueName, is(equalTo(empty())));
	}

	private void popNext(String queueName, Matcher<Optional<Long>> ordinalMatcher) {
		Optional<JobId> jobIdOptional = queueStore().popNextPendingQueueItem(queueName);
		System.out.println(jobIdOptional);

		assertThat(jobIdOptional.map(jobId -> queueTracker.byJobId(jobId).getOrdinal()), ordinalMatcher);
	}

	private void release(Long... items) {
		for (Long itemOrdinal: items)
			queueStore().releaseQueueItem(queueTracker.byOrdinal(itemOrdinal).getJobId());
	}

	private static class SimpleUUIDGenerator implements UUIDGenerator {

		private int counter;

		@Override
		public String newUuid() {
			return Integer.toString(++counter);
		}
	}

	static class QueueTracker {

		private static final Comparator<? super Long> orderingDesc = (o1, o2) -> (o1.compareTo(o2)) * -1;

		private final Map<JobId, QueueItem> itemsByJobId = new HashMap<>();
		private final Map<Long, QueueItem> itemsByOrdinal = new TreeMap<>(orderingDesc);

		void add(QueueItem qi) {
			itemsByJobId.put(qi.getJobId(), qi);
			itemsByOrdinal.put(qi.getOrdinal(), qi);
		}

		QueueItem byJobId(JobId jobId) {
			return itemsByJobId.get(jobId);
		}

		QueueItem byOrdinal(Long ordinal) {
			return itemsByOrdinal.get(ordinal);
		}

		QueueItem lastAdded() {
			return itemsByOrdinal.values().stream().findFirst().orElseThrow(() -> new AssertionError("No queue item found in queue tracker"));
		}
	}
}
