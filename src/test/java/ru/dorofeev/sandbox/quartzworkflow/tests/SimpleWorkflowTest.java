package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.*;
import ru.dorofeev.sandbox.quartzworkflow.Engine;
import ru.dorofeev.sandbox.quartzworkflow.Event;
import ru.dorofeev.sandbox.quartzworkflow.EventHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;

public class SimpleWorkflowTest {

	private static Engine engine = new Engine(org.h2.Driver.class, "jdbc:h2:~/test");

	private static String handlerUri(String localName) {
		return "http://quartzworkflow.sandbox.dorofeev.ru/eventHandlers/" + localName;
	}

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
	public void sanityTest() {
		Model model = new Model();

		engine.registerEventHandlerInstance(handlerUri("addPersonCmd"), new AddPersonCmdHandler(model));
		engine.registerEventHandlerInstance(handlerUri("assignBaseRolesOnPersonAddedEvent"), new AssignBaseRolesOnPersonAddedEventHandler());
		engine.registerEventHandlerInstance(handlerUri("assignRoleCmd"), new AssignRoleCmdHandler(model));
		engine.registerEventHandlerInstance(handlerUri("processRoleAssignmentOnRoleAssignedEvent"), new ProcessRoleAssignmentOnRoleAssignedEventHandler());
		engine.registerEventHandlerInstance(handlerUri("assignAccountCmd"), new AssignAccountCmdHandler(model));

		engine.registerEventHandler(AddPersonCmdEvent.class, handlerUri("addPersonCmd"));
		engine.registerEventHandler(PersonAddedEvent.class, handlerUri("assignBaseRolesOnPersonAddedEvent"));
		engine.registerEventHandler(AssignRoleCmdEvent.class, handlerUri("assignRoleCmd"));
		engine.registerEventHandler(RoleAssignedEvent.class, handlerUri("processRoleAssignmentOnRoleAssignedEvent"));
		engine.registerEventHandler(AssignAccountCmdEvent.class, handlerUri("assignAccountCmd"));

		engine.submitEvent(new AddPersonCmdEvent("john"));

		await().until(() -> model.findPerson("john").isPresent(), is(true));
		await().until(() -> model.findPerson("john").map(Person::getRole), is(Optional.of("baseRole")));
		await().until(() -> model.findPerson("john").map(Person::getAccount), is(Optional.of("baseRole_account")));
	}

	private static class AddPersonCmdEvent extends Event {

		final String personName;

		AddPersonCmdEvent(String personName) {
			this.personName = personName;
		}
	}

	private static class AddPersonCmdHandler implements EventHandler {
		private final Model model;

		AddPersonCmdHandler(Model model) {
			this.model = model;
		}

		@Override
		public void handleEvent(Engine engine, Event event) {
			AddPersonCmdEvent addPersonCmdEvent = (AddPersonCmdEvent) event;
			model.addPerson(addPersonCmdEvent.personName);
			engine.submitEvent(new PersonAddedEvent(addPersonCmdEvent.personName));
		}
	}

	private static class PersonAddedEvent extends Event {

		final String personName;

		PersonAddedEvent(String personName) {
			this.personName = personName;
		}
	}

	private static class AssignBaseRolesOnPersonAddedEventHandler implements EventHandler {

		@Override
		public void handleEvent(Engine engine, Event event) {
			PersonAddedEvent personAddedEvent = (PersonAddedEvent)event;
			engine.submitEvent(new AssignRoleCmdEvent(personAddedEvent.personName, "baseRole"));
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

	private static class AssignRoleCmdHandler implements EventHandler {
		private final Model model;

		AssignRoleCmdHandler(Model model) {
			this.model = model;
		}

		@Override
		public void handleEvent(Engine engine, Event event) {
			AssignRoleCmdEvent assignRoleCmdEvent = (AssignRoleCmdEvent) event;
			Optional<Person> person = model.findPerson(assignRoleCmdEvent.personName);
			if (!person.isPresent())
				throw new RuntimeException("Person " + assignRoleCmdEvent.personName + " not found");

			person.get().setRole(assignRoleCmdEvent.roleName);
			engine.submitEvent(new RoleAssignedEvent(person.get().name, assignRoleCmdEvent.roleName));
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

	private static class ProcessRoleAssignmentOnRoleAssignedEventHandler implements EventHandler {

		@Override
		public void handleEvent(Engine engine, Event event) {
			RoleAssignedEvent roleAssignedEvent = (RoleAssignedEvent) event;
			engine.submitEvent(new AssignAccountCmdEvent(roleAssignedEvent.personName, roleAssignedEvent.roleName + "_account"));
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

	private static class AssignAccountCmdHandler implements EventHandler {

		private final Model model;

		AssignAccountCmdHandler(Model model) {
			this.model = model;
		}

		@Override
		public void handleEvent(Engine engine, Event event) {
			AssignAccountCmdEvent assignAccountCmdEvent = (AssignAccountCmdEvent) event;

			Optional<Person> person = model.findPerson(assignAccountCmdEvent.personName);
			if (!person.isPresent())
				throw new RuntimeException("Person " + assignAccountCmdEvent.personName + " not found");

			person.get().setAccount(assignAccountCmdEvent.accountName);
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
