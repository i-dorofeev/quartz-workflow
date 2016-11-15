package ru.dorofeev.sandbox.quartzworkflow.engine;

import ru.dorofeev.sandbox.quartzworkflow.*;
import ru.dorofeev.sandbox.quartzworkflow.taskrepo.Task;
import ru.dorofeev.sandbox.quartzworkflow.taskrepo.TaskRepository;

public interface Engine {

	rx.Observable<Throwable> errors();

	TaskRepository getTaskRepository();

	Task submitEvent(Event event);

	void retryExecution(TaskId taskId);

	void registerEventHandlerInstance(String handlerUri, EventHandler eventHandler);

	void registerEventHandler(Class<? extends Event> eventType, String handlerUri);

	void registerEventHandler(Class<? extends Event> cmdEventType, EventHandler cmdHandler, String handlerUri);
}
