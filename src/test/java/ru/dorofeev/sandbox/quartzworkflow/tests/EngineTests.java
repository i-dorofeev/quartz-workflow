package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import ru.dorofeev.sandbox.quartzworkflow.Engine;
import ru.dorofeev.sandbox.quartzworkflow.Event;
import ru.dorofeev.sandbox.quartzworkflow.EventHandler;

import static org.awaitility.Awaitility.await;

public class EngineTests {

	private static Engine engine = new Engine();

	@BeforeClass
	public static void beforeClass() throws Exception {
		engine.start();
	}

	@AfterClass
	public static void afterClass() throws Exception {
		engine.shutdown();
	}

	@Test
	public void sanityTest() throws Exception {

		MockEventHandler mockEventHandler = new MockEventHandler();

		engine.registerEventHandlerInstance("http://quartzworkflow.sandbox.dorofeev.ru/eventHandlers/mockEventHandler", mockEventHandler);
		engine.registerEventHandler(Event.class, "http://quartzworkflow.sandbox.dorofeev.ru/eventHandlers/mockEventHandler");

		Event event = new Event();
		engine.submitEvent(event);

		await().until(() -> event.equals(mockEventHandler.getEvent()));
	}

	@SuppressWarnings("WeakerAccess")
	public static class MockEventHandler implements EventHandler {

		private Event event;

		@Override
		public void handleEvent(Engine engine, Event event) {
			this.event = event;
		}

		public Event getEvent() {
			return event;
		}
	}
}
