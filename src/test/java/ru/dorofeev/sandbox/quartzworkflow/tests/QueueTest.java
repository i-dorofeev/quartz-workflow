package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.*;
import ru.dorofeev.sandbox.quartzworkflow.Factory;
import ru.dorofeev.sandbox.quartzworkflow.engine.Engine;
import ru.dorofeev.sandbox.quartzworkflow.engine.Event;
import ru.dorofeev.sandbox.quartzworkflow.engine.TypedEventHandler;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOption;
import ru.dorofeev.sandbox.quartzworkflow.tests.utils.H2Db;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static ru.dorofeev.sandbox.quartzworkflow.engine.EventUtils.noEvents;
import static ru.dorofeev.sandbox.quartzworkflow.taskrepo.Task.Result.CREATED;
import static ru.dorofeev.sandbox.quartzworkflow.taskrepo.Task.Result.RUNNING;

public class QueueTest {

	private static Engine engine;
	private static List<Throwable> errors = new CopyOnWriteArrayList<>();
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

		engine = Factory.createInMemory();
		engine.errors().subscribe(errors::add);
		model = new Model();

		engine.registerEventHandler(IncrementCmdEvent.class, new IncrementCmdHandler(model), handlerUri("incrementCmd"));
		engine.registerEventHandler(VerifyCmdEvent.class, new VerifyCmdHandler(model), handlerUri("verifyCmd"));
	}

	@AfterClass
	public static void afterClass() {
	}

	@Before
	public void beforeTest() {
		errors.clear();
	}

	@After
	public void afterTest() {
		System.out.println("Model: " + model.v1 + "/" + model.v2 + "/" + model.v3);

		assertThat(errors, is(empty()));
		assertThat(engine.getTaskRepository().traverseFailed().collect(toList()), is(empty()));
	}

	@Test
	public void sanityTest() {
		Random random = new Random();
		IntStream.range(0, 50)
			.mapToObj(i -> (random.nextInt(3) == 0) ? new IncrementCmdEvent() : new VerifyCmdEvent())
			.forEach(e -> engine.submitEvent(e));

		await("starting").until(() -> engine.getTaskRepository().traverse(CREATED).count().toBlocking().single(), is(equalTo(0)));
		await("completing").until(() -> engine.getTaskRepository().traverse(RUNNING).count().toBlocking().single(), is(equalTo(0)));
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
