package ru.dorofeev.sandbox.quartzworkflow.tests.queue;

import org.junit.*;
import ru.dorofeev.sandbox.quartzworkflow.Factory;
import ru.dorofeev.sandbox.quartzworkflow.engine.Engine;
import ru.dorofeev.sandbox.quartzworkflow.engine.Event;
import ru.dorofeev.sandbox.quartzworkflow.engine.TypedEventHandler;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions;
import ru.dorofeev.sandbox.quartzworkflow.tests.utils.HSqlServices;
import ru.dorofeev.sandbox.quartzworkflow.utils.RealtimeStopwatchFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static ru.dorofeev.sandbox.quartzworkflow.engine.EventUtils.noEvents;
import static ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorServiceFactory.fixedThreadedExecutorService;
import static ru.dorofeev.sandbox.quartzworkflow.jobs.Job.Result.*;
import static ru.dorofeev.sandbox.quartzworkflow.serialization.SerializationFactory.jsonSerialization;

public class QueueTest {

	private static Engine engine;
	private static HSqlServices hSqlServices;
	private static final List<Throwable> errors = new CopyOnWriteArrayList<>();
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
		hSqlServices = new HSqlServices();

		engine = Factory.spawn(
			jsonSerialization(),
			hSqlServices.jobStoreFactory(),
			hSqlServices.queueStore(),
			fixedThreadedExecutorService(10, 1000, new RealtimeStopwatchFactory()));
		engine.errors().subscribe(errors::add);
		model = new Model();

		engine.registerEventHandler(IncrementCmdEvent.class, new IncrementCmdHandler(model), handlerUri("incrementCmd"));
		engine.registerEventHandler(VerifyCmdEvent.class, new VerifyCmdHandler(model), handlerUri("verifyCmd"));

		engine.start();
	}

	@AfterClass
	public static void afterClass() {
		engine.shutdown();
		hSqlServices.shutdown();
	}

	@Before
	public void beforeTest() {
		errors.clear();
	}

	@After
	public void afterTest() {
		System.out.println("Model: " + model.v1 + "/" + model.v2 + "/" + model.v3);

		assertThat(errors, is(empty()));
		assertThat(engine.getJobRepository().traverseAll(FAILED).toList().toBlocking().single(), is(empty()));
	}

	@Test
	public void sanityTest() {
		IntStream.range(1, 51)
			.mapToObj(i -> ((i % 10) == 0) ? new IncrementCmdEvent() : new VerifyCmdEvent())
			.forEach(e -> engine.submitEvent(e));

		await("start all").until(() -> engine.getJobRepository().traverseAll(CREATED).count().toBlocking().single(), is(equalTo(0)));
		//await("completion").until(() -> engine.getJobRepository().traverseAll(RUNNING).count().toBlocking().single(), is(equalTo(0)));
		await("success").until(() -> engine.getJobRepository().traverseAll(SUCCESS).count().toBlocking().single(), is(equalTo(100)));
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
				Thread.sleep(45);
				model.v2++;
				Thread.sleep(55);
				model.v3++;

				return noEvents();
			} catch (InterruptedException e) {
				return noEvents();
			}
		}

		@Override
		public QueueingOptions getQueueingOption(Event event) {
			return new QueueingOptions("qqq", QueueingOptions.ExecutionType.EXCLUSIVE);
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
				Thread.sleep(22);
				int v2 = model.v2;
				Thread.sleep(27);
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
		public QueueingOptions getQueueingOption(Event event) {
			return new QueueingOptions("qqq", QueueingOptions.ExecutionType.PARALLEL);
		}
	}

}
