package ru.dorofeev.sandbox.quartzworkflow.taskrepo;

public class TaskRepositoryFactory {

	public static TaskRepository create() {
		return new TaskRepositoryImpl();
	}
}
