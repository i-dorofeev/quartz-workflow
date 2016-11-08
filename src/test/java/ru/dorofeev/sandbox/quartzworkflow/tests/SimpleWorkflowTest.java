package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.*;
import ru.dorofeev.sandbox.quartzworkflow.Engine;
import ru.dorofeev.sandbox.quartzworkflow.Event;
import ru.dorofeev.sandbox.quartzworkflow.TaskData;
import ru.dorofeev.sandbox.quartzworkflow.TypedEventHandler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.singletonList;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static ru.dorofeev.sandbox.quartzworkflow.EventUtils.events;
import static ru.dorofeev.sandbox.quartzworkflow.EventUtils.noEvents;
import static ru.dorofeev.sandbox.quartzworkflow.TaskData.Result.FAILED;
import static ru.dorofeev.sandbox.quartzworkflow.tests.Matchers.hasOnlyOneItem;

public class SimpleWorkflowTest {

	private static Engine engine = new Engine(org.h2.Driver.class, "jdbc:h2:~/test");
	private static Model model = new Model();

	@SuppressWarnings("FieldCanBeLocal")
	private static AddPersonCmdHandler addPersonCmdHandler;

	private static AssignRoleCmdHandler assignRoleCmdHandler;

	@SuppressWarnings("FieldCanBeLocal")
	private static AssignAccountCmdHandler assignAccountCmdHandler;

	@SuppressWarnings("FieldCanBeLocal")
	private static AssignBaseRolesOnPersonAddedEventHandler assignBaseRolesOnPersonAddedEventHandler;

	@SuppressWarnings("FieldCanBeLocal")
	private static ProcessRoleAssignmentOnRoleAssignedEventHandler processRoleAssignmentOnRoleAssignedEventHandler;

	private static String handlerUri(String localName) {
		return "http://quartzworkflow.sandbox.dorofeev.ru/eventHandlers/" + localName;
	}

	@BeforeClass
	public static void beforeClass() throws Exception {
		model = new Model();

		addPersonCmdHandler = new AddPersonCmdHandler(model);
		assignRoleCmdHandler = new AssignRoleCmdHandler(model);
		assignAccountCmdHandler = new AssignAccountCmdHandler(model);
		assignBaseRolesOnPersonAddedEventHandler = new AssignBaseRolesOnPersonAddedEventHandler();
		processRoleAssignmentOnRoleAssignedEventHandler = new ProcessRoleAssignmentOnRoleAssignedEventHandler();

		engine.start();

		engine.registerEventHandler(AddPersonCmdEvent.class, addPersonCmdHandler, handlerUri("addPersonCmd"));
		engine.registerEventHandler(AssignRoleCmdEvent.class, assignRoleCmdHandler, handlerUri("assignRoleCmd"));
		engine.registerEventHandler(AssignAccountCmdEvent.class, assignAccountCmdHandler, handlerUri("assignAccountCmd"));
		engine.registerEventHandler(PersonAddedEvent.class, assignBaseRolesOnPersonAddedEventHandler, handlerUri("assignBaseRolesOnPersonAddedEvent"));
		engine.registerEventHandler(RoleAssignedEvent.class, processRoleAssignmentOnRoleAssignedEventHandler, handlerUri("processRoleAssignmentOnRoleAssignedEvent"));
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
	public void sanityTest() {
		engine.submitEvent(new AddPersonCmdEvent("john"));

		await().until(() -> model.findPerson("john").isPresent(), is(true));
		await().until(() -> model.findPerson("john").map(Person::getRole), is(Optional.of("baseRole")));
		await().until(() -> model.findPerson("john").map(Person::getAccount), is(Optional.of("baseRole_account")));
	}

	@Test
	public void faultToleranceTest() {
		assignRoleCmdHandler.setFail(true);

		TaskData td = engine.submitEvent(new AddPersonCmdEvent("james"));
		await().until(() -> model.findPerson("james").isPresent(), is(true));
		await().until(() -> engine.getTaskDataRepo().traverse(td.getId(), FAILED), hasOnlyOneItem());

		List<TaskData> failedTasks = engine.getTaskDataRepo().traverse(td.getId(), FAILED).toList().toBlocking().single();
		assertThat(failedTasks, hasSize(1));

		TaskData failedTask = failedTasks.get(0);
		System.out.println(failedTask.prettyPrint());
		assertThat(failedTask.getException().getMessage(), stringContainsInOrder(singletonList("AssignRoleCmdHandler failed")));
		assignRoleCmdHandler.setFail(false);
		engine.retryExecution(failedTask.getId());

		await().until(() -> model.findPerson("james").map(Person::getRole), is(Optional.of("baseRole")));
		await().until(() -> model.findPerson("james").map(Person::getAccount), is(Optional.of("baseRole_account")));
	}

	private static class AddPersonCmdEvent extends Event {

		final String personName;

		AddPersonCmdEvent(String personName) {
			this.personName = personName;
		}
	}

	private static class AddPersonCmdHandler extends TypedEventHandler<AddPersonCmdEvent> {
		private final Model model;

		AddPersonCmdHandler(Model model) {
			this.model = model;
		}

		@Override
		protected List<Event> handle(AddPersonCmdEvent event) {
			model.addPerson(event.personName);
			return events(new PersonAddedEvent(event.personName));
		}
	}

	private static class PersonAddedEvent extends Event {

		final String personName;

		PersonAddedEvent(String personName) {
			this.personName = personName;
		}
	}

	private static class AssignBaseRolesOnPersonAddedEventHandler extends TypedEventHandler<PersonAddedEvent> {

		@Override
		protected List<Event> handle(PersonAddedEvent event) {
			return events(new AssignRoleCmdEvent(event.personName, "baseRole"));
		}
	}

	private static class AssignRoleCmdEvent extends Event {

		final String personName;
		final String roleName;

		AssignRoleCmdEvent(String personName, String roleName) {
			this.personName = personName;
			this.roleName = roleName;
		}
	}

	private static class AssignRoleCmdHandler extends TypedEventHandler<AssignRoleCmdEvent> {
		private final Model model;
		private boolean fail;

		AssignRoleCmdHandler(Model model) {
			this.model = model;
		}

		void setFail(boolean fail) {
			this.fail = fail;
		}

		@Override
		public List<Event> handle(AssignRoleCmdEvent event) {
			if (fail)
				throw new RuntimeException("AssignRoleCmdHandler failed");

			Optional<Person> person = model.findPerson(event.personName);
			if (!person.isPresent())
				throw new RuntimeException("Person " + event.personName + " not found");

			person.get().setRole(event.roleName);
			return events(new RoleAssignedEvent(person.get().name, event.roleName));
		}
	}

	private static class RoleAssignedEvent extends Event {

		final String personName;
		final String roleName;

		RoleAssignedEvent(String personName, String roleName) {
			this.personName = personName;
			this.roleName = roleName;
		}
	}

	private static class ProcessRoleAssignmentOnRoleAssignedEventHandler extends TypedEventHandler<RoleAssignedEvent> {

		@Override
		protected List<Event> handle(RoleAssignedEvent event) {
			return events(new AssignAccountCmdEvent(event.personName, event.roleName + "_account"));
		}
	}

	private static class AssignAccountCmdEvent extends Event {

		final String personName;
		final String accountName;

		AssignAccountCmdEvent(String personName, String accountName) {
			this.personName = personName;
			this.accountName = accountName;
		}
	}

	private static class AssignAccountCmdHandler extends TypedEventHandler<AssignAccountCmdEvent> {

		private final Model model;

		AssignAccountCmdHandler(Model model) {
			this.model = model;
		}

		@Override
		protected List<Event> handle(AssignAccountCmdEvent event) {
			Optional<Person> person = model.findPerson(event.personName);
			if (!person.isPresent())
				throw new RuntimeException("Person " + event.personName + " not found");

			person.get().setAccount(event.accountName);
			return noEvents();
		}
	}

	private static class Model {

		private final Map<String, Person> people = new HashMap<>();

		Optional<Person> findPerson(String personName) {
			return Optional.ofNullable(people.get(personName));
		}

		void addPerson(String personName) {
			Person person = new Person(personName);
			people.put(personName, person);
		}
	}

	private static class Person {

		private final String name;
		private String role;
		private String account;

		private Person(String name) {
			this.name = name;
		}

		String getRole() {
			return role;
		}

		void setRole(String role) {
			this.role = role;
		}

		String getAccount() {
			return account;
		}

		void setAccount(String account) {
			this.account = account;
		}
	}
}
