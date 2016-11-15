package ru.dorofeev.sandbox.quartzworkflow.engine;

import ru.dorofeev.sandbox.quartzworkflow.JobKey;
import ru.dorofeev.sandbox.quartzworkflow.TaskId;
import ru.dorofeev.sandbox.quartzworkflow.execution.Executable;
import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager;
import ru.dorofeev.sandbox.quartzworkflow.taskrepo.Task;
import ru.dorofeev.sandbox.quartzworkflow.taskrepo.TaskRepository;
import ru.dorofeev.sandbox.quartzworkflow.utils.ErrorObservable;
import rx.Observable;
import rx.subjects.PublishSubject;

import java.util.*;

import static ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService.scheduleTaskCmd;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager.enqueueCmd;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager.giveMeMoreCmd;

class EngineImpl implements Engine {

	private Map<Class<? extends Event>, Set<String>> eventHandlers = new HashMap<>();
	private Map<String, EventHandler> eventHandlerInstances = new HashMap<>();

	private final TaskRepository taskRepository;

	private ErrorObservable errors = new ErrorObservable();
	private final JobKey scheduleEventHandlersJob = new JobKey("scheduleEventHandlersJob");
	private final JobKey executeEventHandlerJob = new JobKey("executeEventHandlerJob");

	@SuppressWarnings("FieldCanBeLocal")
	private final PublishSubject<TaskRepository.Cmd> taskRepositoryCmds = PublishSubject.create();

	@SuppressWarnings("FieldCanBeLocal")
	private final PublishSubject<QueueManager.Cmd> queueManagerCmds = PublishSubject.create();

	@SuppressWarnings("FieldCanBeLocal")
	private final PublishSubject<ExecutorService.Cmd> executorServiceCmds = PublishSubject.create();


	EngineImpl(TaskRepository taskRepository, ExecutorService executorService, QueueManager queueManager) {
		this.taskRepository = taskRepository;

		this.errors.subscribeTo(taskRepository.getErrors());
		this.errors.subscribeTo(executorService.getErrors());
		this.errors.subscribeTo(queueManager.getErrors());

		Observable<TaskRepository.Event> taskRepositoryOutput = taskRepositoryCmds.compose(this.taskRepository::bind);
		taskRepositoryOutput
			.compose(errors.filterMapRetry(TaskRepository.Event::isAdd, this::asEnqueueCmd))
			.subscribe(queueManagerCmds);

		taskRepositoryOutput
			.compose(errors.filterMapRetry(TaskRepository.Event::isComplete, this::asNotifyCompletedCmd))
			.subscribe(queueManagerCmds);

		Observable<QueueManager.Event> queueManagerOutput = queueManagerCmds.compose(queueManager::bind);
		queueManagerOutput
			.compose(errors.filterMapRetry(QueueManager.TaskPoppedEvent.class, this::asScheduleTaskCmd))
			.subscribe(executorServiceCmds);

		rx.Observable<ExecutorService.Event> executorServiceOutput = executorServiceCmds.compose(executorService::bind);
		executorServiceOutput
			.compose(errors.filterMapRetry(ExecutorService.TaskCompletedEvent.class, this::asCompleteTaskCmd))
			.subscribe(taskRepositoryCmds);

		executorServiceOutput
			.compose(errors.filterMapRetry(ExecutorService.IdleEvent.class, this::asGiveMeMoreCmd))
			.subscribe(queueManagerCmds);
	}



	private TaskRepository.CompleteTaskCmd asCompleteTaskCmd(ExecutorService.TaskCompletedEvent event) {
		return new TaskRepository.CompleteTaskCmd(event.getTaskId(), event.getException());
	}

	private ExecutorService.ScheduleTaskCmd asScheduleTaskCmd(QueueManager.TaskPoppedEvent event) {
		TaskId taskId = event.getTaskId();

		Task task = taskRepository.findTask(taskId).orElseThrow(() -> new EngineException("Couldn't find task " + taskId));
		Executable executable = getExecutable(task);
		return scheduleTaskCmd(taskId, task.getJobData(), executable);
	}

	private Executable getExecutable(Task task) {
		if (task.getJobKey().equals(scheduleEventHandlersJob)) {
			return new ScheduleEventHandlersJob(this);
		} else if (task.getJobKey().equals(executeEventHandlerJob)) {
			return new ExecuteEventHandlerJob(this);
		} else {
			throw new EngineException("Unknown job key " + task.getJobKey());
		}
	}

	private QueueManager.NotifyCompletedCmd asNotifyCompletedCmd(TaskRepository.Event event) {
		return new QueueManager.NotifyCompletedCmd(event.getTask().getId());
	}

	private QueueManager.EnqueueCmd asEnqueueCmd(TaskRepository.Event event) {
		return new QueueManager.EnqueueCmd(event.getTask().getQueueName(), event.getTask().getExecutionType(), event.getTask().getId());
	}

	private QueueManager.Cmd asGiveMeMoreCmd(ExecutorService.Event event) {
		return giveMeMoreCmd();
	}

	@Override
	public rx.Observable<Throwable> errors() {
		return this.errors.asObservable();
	}

	@Override
	public TaskRepository getTaskRepository() {
		return taskRepository;
	}

	@Override
	public Task submitEvent(Event event) {
		return submitEvent(/* parentId */ null, event);
	}

	Task submitEvent(TaskId parentId, Event event) {
		return taskRepository.addTask(parentId, scheduleEventHandlersJob, ScheduleEventHandlersJob.params(event), /* queueingOption */ null);
	}

	@Override
	public void retryExecution(TaskId taskId) {
		queueManagerCmds.onNext(enqueueCmd(taskId));
	}

	@Override
	public void registerEventHandlerInstance(String handlerUri, EventHandler eventHandler) {
		EventHandler replacedHandler = eventHandlerInstances.putIfAbsent(handlerUri, eventHandler);
		if (replacedHandler != null)
			throw new EngineException("Event handler " + eventHandler + " is already registered for uri " + handlerUri);
	}

	@Override
	public void registerEventHandler(Class<? extends Event> eventType, String handlerUri) {
		Set<String> handlersForEventType = eventHandlers.get(eventType);
		if (handlersForEventType == null) {
			handlersForEventType = new HashSet<>();
			eventHandlers.put(eventType, handlersForEventType);
		}

		handlersForEventType.add(handlerUri);
	}

	@Override
	public void registerEventHandler(Class<? extends Event> cmdEventType, EventHandler cmdHandler, String handlerUri) {
		registerEventHandlerInstance(handlerUri, cmdHandler);
		registerEventHandler(cmdEventType, handlerUri);
	}

	void submitHandler(TaskId parentId, Event event, String handlerUri) {
		Optional<EventHandler> handlerByUriOpt = findHandlerByUri(handlerUri);
		EventHandler eventHandler = handlerByUriOpt.orElseThrow(() -> new EngineException("Handler instance for URI " + handlerUri + " not found"));

		taskRepositoryCmds.onNext(new TaskRepository.AddTaskCmd(parentId, executeEventHandlerJob,
			ExecuteEventHandlerJob.params(event, handlerUri),
			eventHandler.getQueueingOption(event)));
	}

	Optional<EventHandler> findHandlerByUri(String handlerUri) {
		EventHandler eventHandler = eventHandlerInstances.get(handlerUri);
		return Optional.ofNullable(eventHandler);
	}

	Set<String> findHandlers(Class<? extends Event> eventType) {
		Set<String> handlers = eventHandlers.get(eventType);
		if (handlers == null)
			return Collections.emptySet();
		else
			return Collections.unmodifiableSet(handlers);
	}
}
