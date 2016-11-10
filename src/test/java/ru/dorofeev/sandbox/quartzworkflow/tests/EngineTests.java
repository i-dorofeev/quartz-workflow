package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.*;
import ru.dorofeev.sandbox.quartzworkflow.Engine;
import ru.dorofeev.sandbox.quartzworkflow.Event;
import ru.dorofeev.sandbox.quartzworkflow.EventHandler;
import ru.dorofeev.sandbox.quartzworkflow.QueueingOption;

import java.util.List;

import static org.awaitility.Awaitility.await;
import static ru.dorofeev.sandbox.quartzworkflow.EventUtils.noEvents;

public class EngineTests {

	private static Engine engine;

	@BeforeClass
	public static void beforeClass() throws Exception {
		H2Db h2Db = new H2Db("./build/test");
		h2Db.deleteDb();

		engine = new Engine(org.h2.Driver.class, h2Db.jdbcUrl());
		engine.start();
	}

	@AfterClass
	public static void afterClass() throws Exception {
		engine.shutdown();
	}

	@Before
	public void beforeTest() {
		engine.resetErrors();
	}

	@After
	public void afterTest() {
		engine.assertSuccess();
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
