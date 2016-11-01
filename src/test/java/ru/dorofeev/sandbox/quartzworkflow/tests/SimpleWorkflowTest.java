package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.*;
import ru.dorofeev.sandbox.quartzworkflow.Engine;
import ru.dorofeev.sandbox.quartzworkflow.Event;
import ru.dorofeev.sandbox.quartzworkflow.TypedEventHandler;

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

		engine.registerEventHandler(AddPersonCmdEvent.class, new AddPersonCmdHandler(model), handlerUri("addPersonCmd"));
		engine.registerEventHandler(AssignRoleCmdEvent.class, new AssignRoleCmdHandler(model), handlerUri("assignRoleCmd"));
		engine.registerEventHandler(AssignAccountCmdEvent.class, new AssignAccountCmdHandler(model), handlerUri("assignAccountCmd"));
		engine.registerEventHandler(PersonAddedEvent.class, new AssignBaseRolesOnPersonAddedEventHandler(), handlerUri("assignBaseRolesOnPersonAddedEvent"));
		engine.registerEventHandler(RoleAssignedEvent.class, new ProcessRoleAssignmentOnRoleAssignedEventHandler(), handlerUri("processRoleAssignmentOnRoleAssignedEvent"));

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

	private static class AddPersonCmdHandler extends TypedEventHandler<AddPersonCmdEvent> {
		private final Model model;

		AddPersonCmdHandler(Model model) {
			this.model = model;
		}

		@Override
		protected void handle(Engine engine, AddPersonCmdEvent event) {
			model.addPerson(event.personName);
			engine.submitEvent(new PersonAddedEvent(event.personName));
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
		protected void handle(Engine engine, PersonAddedEvent event) {
			engine.submitEvent(new AssignRoleCmdEvent(event.personName, "baseRole"));
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

		AssignRoleCmdHandler(Model model) {
			this.model = model;
		}

		@Override
		public void handle(Engine engine, AssignRoleCmdEvent event) {
			Optional<Person> person = model.findPerson(event.personName);
			if (!person.isPresent())
				throw new RuntimeException("Person " + event.personName + " not found");

			person.get().setRole(event.roleName);
			engine.submitEvent(new RoleAssignedEvent(person.get().name, event.roleName));
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
		protected void handle(Engine engine, RoleAssignedEvent event) {
			engine.submitEvent(new AssignAccountCmdEvent(event.personName, event.roleName + "_account"));
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
		protected void handle(Engine engine, AssignAccountCmdEvent event) {
			Optional<Person> person = model.findPerson(event.personName);
			if (!person.isPresent())
				throw new RuntimeException("Person " + event.personName + " not found");

			person.get().setAccount(event.accountName);
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
