package ru.dorofeev.sandbox.quartzworkflow.tests.queue;

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
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.EXCLUSIVE;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.PARALLEL;
import static rx.Observable.from;

@FixMethodOrder(NAME_ASCENDING)
public abstract class AbstractQueueStoreTest {

	protected static final QueueTracker queueTracker = new QueueTracker();
	protected static final UUIDGenerator uuidGenerator = new SimpleUUIDGenerator();

	@Rule
	public final ExpectedException expectedException = ExpectedException.none();

	protected abstract QueueStore queueStore();

	@Test
	public void test010_initialData() {
		from(new QueueingOptions.ExecutionType[] {
			PARALLEL,
			PARALLEL,
			PARALLEL,
			PARALLEL,
			EXCLUSIVE,
			EXCLUSIVE,
			PARALLEL,
			PARALLEL,
			PARALLEL })
			.map(et -> queueStore().insertQueueItem(new JobId(uuidGenerator.newUuid()), null, et))
			.forEach(queueTracker::add);
	}

	@Test
	public void test020_popNext() {

		popNext(asList(1L, 2L, 3L, 4L));
		popNext(emptyList());
	}

	@Test
	public void test030_removeCompleted() {

		queueStore().removeQueueItem(queueTracker.byOrdinal(1L).getJobId());
		queueStore().removeQueueItem(queueTracker.byOrdinal(2L).getJobId());
		queueStore().removeQueueItem(queueTracker.byOrdinal(3L).getJobId());
		queueStore().removeQueueItem(queueTracker.byOrdinal(4L).getJobId());

		popNext(singletonList(5L));

		queueStore().removeQueueItem(queueTracker.byOrdinal(5L).getJobId());

		popNext(singletonList(6L));

		queueStore().removeQueueItem(queueTracker.byOrdinal(6L).getJobId());

		popNext(asList(7L, 8L, 9L));
	}

	@Test
	public void test040_cannotEnqueueSameJobTwice() {
		expectedException.expect(QueueStoreException.class);
		expectedException.expectMessage(containsString("is already enqueued"));

		QueueItem queueItem = queueTracker.lastAdded().orElseThrow(() -> new AssertionError("No queue item found in queue tracker"));
		queueStore().insertQueueItem(queueItem.getJobId(), null, PARALLEL);
	}

	private void popNext(List<Long> expectedItems) {
		System.out.println("Getting next " + expectedItems.size() + " queue items...");
		for (int i = 0; i < expectedItems.size(); i++) {
			Optional<JobId> jobIdOptional = queueStore().popNextPendingQueueItem(null);
			System.out.println(i + ": " + jobIdOptional);

			JobId jobId = jobIdOptional.orElseThrow(() -> new AssertionError("Unexpected empty jobId"));

			assertThat(queueTracker.byJobId(jobId).getOrdinal(), is(equalTo(expectedItems.get(i))));
		}
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

		Optional<QueueItem> lastAdded() {
			return itemsByOrdinal.values().stream().findFirst();
		}

	}
}
