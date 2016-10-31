package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.quartz.Scheduler;
import org.quartz.impl.StdSchedulerFactory;
import ru.dorofeev.sandbox.quartzworkflow.Engine;
import ru.dorofeev.sandbox.quartzworkflow.Event;
import ru.dorofeev.sandbox.quartzworkflow.EventHandler;

public class TestClass {

	private static Scheduler scheduler;
	private static Engine engine;

	@BeforeClass
	public static void beforeClass() throws Exception {
		scheduler = StdSchedulerFactory.getDefaultScheduler();
		scheduler.start();

		engine = new Engine(scheduler);
	}

	@AfterClass
	public static void afterClass() throws Exception {
		scheduler.shutdown(true);
	}


	@Test
	public void test() throws Exception {

		engine.registerEventHandler(Event.class, LoggingEventHandler.class);

		engine.submitEvent(new Event());

		Thread.sleep(1000);
	}

	@SuppressWarnings("WeakerAccess")
	public static class LoggingEventHandler implements EventHandler {

		@Override
		public void handleEvent(Engine engine, Event event) {
			System.out.println(event);
		}
	}
}
