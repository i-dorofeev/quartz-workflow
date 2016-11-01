package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.*;
import ru.dorofeev.sandbox.quartzworkflow.Engine;
import ru.dorofeev.sandbox.quartzworkflow.Event;
import ru.dorofeev.sandbox.quartzworkflow.EventHandler;

import static org.awaitility.Awaitility.await;

public class EngineTests {

	private static Engine engine = new Engine(org.h2.Driver.class, "jdbc:h2:~/test");

	@BeforeClass
	public static void beforeClass() throws Exception {
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
		public void handleEvent(Engine engine, Event event) {
			this.event = event;
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
