package ru.dorofeev.sandbox.quartzworkflow.tests.queue;

import org.hamcrest.Matcher;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.NodeId;
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
import static ru.dorofeev.sandbox.quartzworkflow.NodeId.ANY_NODE;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.EXCLUSIVE;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.PARALLEL;

@FixMethodOrder(NAME_ASCENDING)
public abstract class AbstractQueueStoreTest {

	private static final QueueTracker queueTracker = new QueueTracker();
	private static final UUIDGenerator uuidGenerator = new SimpleUUIDGenerator();
	private static final NodeId node1 = new NodeId("node1");
	private static final NodeId node2 = new NodeId("node2");

	@Rule
	public final ExpectedException expectedException = ExpectedException.none();

	protected abstract QueueStore queueStore();

	@Test
	public void test050_initialData() {
		insertQueueItem("q1", PARALLEL, node1);	// 1L
		insertQueueItem("q1", PARALLEL, node1);	// 2L
		insertQueueItem("q1", PARALLEL, ANY_NODE);	// 3L
		insertQueueItem("q2", PARALLEL, node2);	// 4L 4
		insertQueueItem("q2", PARALLEL, node2);	// 5L 5
		insertQueueItem("q2", PARALLEL, ANY_NODE);	// 6L
		insertQueueItem("q1", EXCLUSIVE, ANY_NODE);	// 7L 7
		insertQueueItem("q2", EXCLUSIVE, ANY_NODE);	// 8L 8

		insertQueueItem("q3", PARALLEL, node1);	// 9L 9
		insertQueueItem("q3", PARALLEL, node2);	// 10L 10
		insertQueueItem("q4", PARALLEL, node1);	// 11L 11
		insertQueueItem("q4", PARALLEL, node2);	// 12L 12
		insertQueueItem("q3", EXCLUSIVE, ANY_NODE);	// 13L 13
		insertQueueItem("q4", EXCLUSIVE, ANY_NODE);	// 14L 14
	}

	@Test
	public void test060_cannotEnqueueSameJobTwice() {
		expectedException.expect(QueueStoreException.class);
		expectedException.expectMessage(containsString("is already enqueued"));

		queueStore().insertQueueItem(queueTracker.lastAdded().getJobId(), "q9", PARALLEL, ANY_NODE);
	}

	@Test
	public void test070_popParallel() {

		popNext("q1", node2, singletonList(3L));
		popNext("q1", node1, asList(1L, 2L));

		popNext("q2", node1, singletonList(6L));
		popNext("q2", node2, asList(4L, 5L));
	}

	@Test
	public void test080_releasePoppedAndPopExclusive() {
		release(1L, 2L, 3L);
		popNext("q1", node1 , singletonList(7L));

		release(4L, 5L, 6L);
		popNext("q2", node2, singletonList(8L));
	}

	@Test
	public void test090_popParallelAllQueues() {
		release(7L, 8L);

		popNext(null, node1, asList(9L, 11L));
		popNext(null, node2, asList(10L, 12L));
	}

	@Test
	public void test100_releasePoppedAndPopExclusive_allQueues() {
		release(9L, 10L, 11L, 12L);
		popNext(null, node1, asList(13L, 14L));
	}

	private void insertQueueItem(String queueName, QueueingOptions.ExecutionType executionType, NodeId nodeId) {
		queueTracker.add(queueStore().insertQueueItem(newJobId(), queueName, executionType, nodeId));
	}

	private JobId newJobId() {
		return new JobId(uuidGenerator.newUuid());
	}

	private void popNext(String queueName, NodeId nodeId, List<Long> expectedItems) {
		System.out.println("Getting next " + expectedItems.size() + " queue items...");

		for (Long expectedItem : expectedItems)
			popNext(queueName, nodeId, is(equalTo(of(expectedItem))));

		popNext(queueName, nodeId, is(equalTo(empty())));
	}

	private void popNext(String queueName, NodeId node, Matcher<Optional<Long>> ordinalMatcher) {
		Optional<JobId> jobIdOptional = queueStore().popNextPendingQueueItem(queueName, node);
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
