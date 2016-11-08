package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.*;
import ru.dorofeev.sandbox.quartzworkflow.Engine;
import ru.dorofeev.sandbox.quartzworkflow.Event;
import ru.dorofeev.sandbox.quartzworkflow.QueueingOption;
import ru.dorofeev.sandbox.quartzworkflow.TypedEventHandler;

import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static ru.dorofeev.sandbox.quartzworkflow.EventUtils.noEvents;
import static ru.dorofeev.sandbox.quartzworkflow.TaskData.Result.CREATED;
import static ru.dorofeev.sandbox.quartzworkflow.TaskData.Result.RUNNING;

public class QueueTest {

	private static Engine engine;
	private static Model model;

	private static class Model {
		volatile int v1;
		volatile int v2;
		volatile int v3;
	}

	private static String handlerUri(String localName) {
		return "http://quartzworkflow.sandbox.dorofeev.ru/queueTest/eventHandlers/" + localName;
	}

	@BeforeClass
	public static void beforeClass() {
		H2Db h2Db = new H2Db("./build/test");
		h2Db.deleteDb();

		engine = new Engine(org.h2.Driver.class, h2Db.jdbcUrl());
		model = new Model();

		engine.registerEventHandler(IncrementCmdEvent.class, new IncrementCmdHandler(model), handlerUri("incrementCmd"));
		engine.registerEventHandler(VerifyCmdEvent.class, new VerifyCmdHandler(model), handlerUri("verifyCmd"));

		engine.start();
	}

	@AfterClass
	public static void afterClass() {
		engine.shutdown();
	}

	@Before
	public void beforeTest() {
		engine.resetErrors();
	}

	@After
	public void afterTest() {
		System.out.println("Model: " + model.v1 + "/" + model.v2 + "/" + model.v3);
		engine.assertSuccess();
	}

	@Test
	public void sanityTest() {
		Random random = new Random();
		IntStream.range(0, 50)
			.mapToObj(i -> (random.nextInt(3) == 0) ? new IncrementCmdEvent() : new VerifyCmdEvent())
			.forEach(e -> engine.submitEvent(e));

		await("starting").until(() -> engine.getTaskDataRepo().traverse(CREATED).count().toBlocking().single(), is(equalTo(0)));
		await("completing").until(() -> engine.getTaskDataRepo().traverse(RUNNING).count().toBlocking().single(), is(equalTo(0)));
	}

	private static class IncrementCmdEvent extends Event { }

	private static class VerifyCmdEvent extends Event { }

	private static class IncrementCmdHandler extends TypedEventHandler<IncrementCmdEvent> {

		private final Model model;

		private IncrementCmdHandler(Model model) {
			this.model = model;
		}

		@Override
		protected List<Event> handle(IncrementCmdEvent event) {
			try {
				model.v1++;
				Thread.sleep(90);
				model.v2++;
				Thread.sleep(110);
				model.v3++;

				return noEvents();
			} catch (InterruptedException e) {
				return noEvents();
			}
		}

		@Override
		public QueueingOption getQueueingOption(Event event) {
			return new QueueingOption("qqq", QueueingOption.ExecutionType.EXCLUSIVE);
		}
	}

	private static class VerifyCmdHandler extends TypedEventHandler<VerifyCmdEvent> {

		private final Model model;

		private VerifyCmdHandler(Model model) {
			this.model = model;
		}

		@Override
		protected List<Event> handle(VerifyCmdEvent event) {
			try {
				int v1 = model.v1;
				Thread.sleep(45);
				int v2 = model.v2;
				Thread.sleep(55);
				int v3 = model.v3;

				if (v1 != v2 || v1 != v3)
					throw new AssertionError("Values do not match: " + v1 + "/" + v2 + "/" + v3);
				else
					return noEvents();
			} catch (InterruptedException e) {
				return noEvents();
			}
		}

		@Override
		public QueueingOption getQueueingOption(Event event) {
			return new QueueingOption("qqq", QueueingOption.ExecutionType.PARALLEL);
		}
	}

}
