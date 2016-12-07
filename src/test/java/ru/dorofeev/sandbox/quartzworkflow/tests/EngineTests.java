package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.*;
import ru.dorofeev.sandbox.quartzworkflow.NodeId;
import ru.dorofeev.sandbox.quartzworkflow.engine.Engine;
import ru.dorofeev.sandbox.quartzworkflow.engine.Event;
import ru.dorofeev.sandbox.quartzworkflow.engine.EventHandler;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions;
import ru.dorofeev.sandbox.quartzworkflow.tests.utils.HSqlServices;
import ru.dorofeev.sandbox.quartzworkflow.tests.utils.Utils;
import ru.dorofeev.sandbox.quartzworkflow.utils.RealtimeStopwatchFactory;
import ru.dorofeev.sandbox.quartzworkflow.utils.SystemClock;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static ru.dorofeev.sandbox.quartzworkflow.Factory.spawn;
import static ru.dorofeev.sandbox.quartzworkflow.engine.EventUtils.noEvents;
import static ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorServiceFactory.fixedThreadedExecutorService;
import static ru.dorofeev.sandbox.quartzworkflow.jobs.Job.Result.FAILED;
import static ru.dorofeev.sandbox.quartzworkflow.serialization.SerializationFactory.jsonSerialization;

public class EngineTests {

	private static Engine engine;
	private static HSqlServices hSqlServices;
	private static final List<String> errors = new CopyOnWriteArrayList<>();

	@BeforeClass
	public static void beforeClass() throws Exception {
		hSqlServices = new HSqlServices();

		engine = spawn(
			new NodeId("EngineTests"),
			jsonSerialization(),
			hSqlServices.jobStoreFactory(),
			hSqlServices.queueStore(),
			fixedThreadedExecutorService(10, 1000, new RealtimeStopwatchFactory(), new SystemClock()));
		engine.errors().map(Utils::exceptionToString).subscribe(errors::add);

		engine.start();
	}

	@AfterClass
	public static void afterClass() throws Exception {
		engine.shutdown();
		hSqlServices.shutdown();
	}

	@Before
	public void beforeTest() {
		errors.clear();
	}

	@After
	public void afterTest() {
		assertThat(errors, is(empty()));
		assertThat(engine.getJobRepository().traverseAll(FAILED).toList().toBlocking().single(), is(empty()));
	}

	@Test
	public void sanityTest() throws Exception {

		MockEventHandler mockEventHandler = new MockEventHandler();

		engine.registerEventHandlerInstance("http://quartzworkflow.sandbox.dorofeev.ru/eventHandlers/mockEventHandler", mockEventHandler);
		engine.registerEventHandler(StubEvent.class, "http://quartzworkflow.sandbox.dorofeev.ru/eventHandlers/mockEventHandler");

		StubEvent event = new StubEvent("test");
		engine.submitEvent(event);

		await().until(() -> event.equals(mockEventHandler.getEvent()));
	}

	private static class MockEventHandler implements EventHandler {

		private Event event;

		@Override
		public List<Event> handleEvent(Event event) {
			this.event = event;
			return noEvents();
		}

		@Override
		public QueueingOptions getQueueingOption(Event event) {
			return QueueingOptions.DEFAULT;
		}

		Event getEvent() {
			return event;
		}
	}

	private static class StubEvent extends Event {

		private final String id;

		StubEvent(String id) {
			this.id = id;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			StubEvent stubEvent = (StubEvent) o;

			return id != null ? id.equals(stubEvent.id) : stubEvent.id == null;

		}

		@Override
		public int hashCode() {
			return id != null ? id.hashCode() : 0;
		}
	}
}
