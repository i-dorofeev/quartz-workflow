package ru.dorofeev.sandbox.quartzworkflow.engine;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.execution.Executable;
import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObjectFactory;
import ru.dorofeev.sandbox.quartzworkflow.taskrepo.Task;
import ru.dorofeev.sandbox.quartzworkflow.taskrepo.TaskRepository;
import ru.dorofeev.sandbox.quartzworkflow.utils.ErrorObservable;
import rx.Observable;
import rx.subjects.PublishSubject;

import java.util.*;

import static ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService.scheduleTaskCmd;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager.enqueueCmd;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager.giveMeMoreCmd;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager.notifyCompletedCmd;
import static ru.dorofeev.sandbox.quartzworkflow.taskrepo.TaskRepository.addTaskCmd;
import static ru.dorofeev.sandbox.quartzworkflow.taskrepo.TaskRepository.completeTaskCmd;

class EngineImpl implements Engine {

	private Map<Class<? extends Event>, Set<String>> eventHandlers = new HashMap<>();
	private Map<String, EventHandler> eventHandlerInstances = new HashMap<>();

	private final TaskRepository taskRepository;
	private final SerializedObjectFactory serializedObjectFactory;

	private ErrorObservable errors = new ErrorObservable();

	@SuppressWarnings("FieldCanBeLocal")
	private final PublishSubject<TaskRepository.Cmd> taskRepositoryCmds = PublishSubject.create();

	@SuppressWarnings("FieldCanBeLocal")
	private final PublishSubject<QueueManager.Cmd> queueManagerCmds = PublishSubject.create();

	@SuppressWarnings("FieldCanBeLocal")
	private final PublishSubject<ExecutorService.Cmd> executorServiceCmds = PublishSubject.create();


	EngineImpl(TaskRepository taskRepository, ExecutorService executorService, QueueManager queueManager, SerializedObjectFactory serializedObjectFactory) {
		this.taskRepository = taskRepository;
		this.serializedObjectFactory = serializedObjectFactory;

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
		return completeTaskCmd(event.getJobId(), event.getException());
	}

	private ExecutorService.ScheduleTaskCmd asScheduleTaskCmd(QueueManager.TaskPoppedEvent event) {
		JobId jobId = event.getJobId();

		Task task = taskRepository.findTask(jobId).orElseThrow(() -> new EngineException("Couldn't find task " + jobId));
		Executable executable = getExecutable(task);
		return scheduleTaskCmd(jobId, task.getArgs(), executable);
	}

	private Executable getExecutable(Task task) {
		if (task.getJobKey().equals(SCHEDULE_EVENT_HANDLERS_JOB)) {
			return new ScheduleEventHandlersJob(this);
		} else if (task.getJobKey().equals(EXECUTE_EVENT_HANDLER_JOB)) {
			return new ExecuteEventHandlerJob(this);
		} else {
			throw new EngineException("Unknown job key " + task.getJobKey());
		}
	}

	private QueueManager.NotifyCompletedCmd asNotifyCompletedCmd(TaskRepository.Event event) {
		return notifyCompletedCmd(event.getTask().getId());
	}

	private QueueManager.EnqueueCmd asEnqueueCmd(TaskRepository.Event event) {
		return enqueueCmd(event.getTask().getQueueName(), event.getTask().getExecutionType(), event.getTask().getId());
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

	Task submitEvent(JobId parentId, Event event) {
		return taskRepository.addTask(
			parentId, SCHEDULE_EVENT_HANDLERS_JOB,
			new ScheduleEventHandlersJob.Args(event).serialize(serializedObjectFactory), /* queueingOption */ null);
	}

	@Override
	public void retryExecution(JobId jobId) {
		queueManagerCmds.onNext(enqueueCmd(jobId));
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

	void submitHandler(JobId parentId, Event event, String handlerUri) {
		Optional<EventHandler> handlerByUriOpt = findHandlerByUri(handlerUri);
		EventHandler eventHandler = handlerByUriOpt.orElseThrow(() -> new EngineException("Handler instance for URI " + handlerUri + " not found"));

		taskRepositoryCmds.onNext(
			addTaskCmd(
				parentId, EXECUTE_EVENT_HANDLER_JOB,
				new ExecuteEventHandlerJob.Args(handlerUri, event).serialize(serializedObjectFactory),
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
