package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.*;
import ru.dorofeev.sandbox.quartzworkflow.Factory;
import ru.dorofeev.sandbox.quartzworkflow.engine.Engine;
import ru.dorofeev.sandbox.quartzworkflow.engine.Event;
import ru.dorofeev.sandbox.quartzworkflow.engine.EventHandler;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOption;
import ru.dorofeev.sandbox.quartzworkflow.tests.utils.H2Db;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.stream.Collectors.toList;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static ru.dorofeev.sandbox.quartzworkflow.engine.EventUtils.noEvents;

public class EngineTests {

	private static Engine engine;
	private static List<Throwable> errors = new CopyOnWriteArrayList<>();

	@BeforeClass
	public static void beforeClass() throws Exception {
		H2Db h2Db = new H2Db("./build/test");
		h2Db.deleteDb();

		engine = Factory.createInMemory();
		engine.errors().subscribe(errors::add);
	}

	@AfterClass
	public static void afterClass() throws Exception {
	}

	@Before
	public void beforeTest() {
		errors.clear();
	}

	@After
	public void afterTest() {
		assertThat(errors, is(empty()));
		assertThat(engine.getJobRepository().traverseFailed().collect(toList()), is(empty()));
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
		public QueueingOption getQueueingOption(Event event) {
			return null;
		}

		Event getEvent() {
			return event;
		}
	}

	private static class StubEvent extends Event {

		private String id;

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
