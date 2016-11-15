package ru.dorofeev.sandbox.quartzworkflow;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import ru.dorofeev.sandbox.quartzworkflow.ExecutorService.IdleEvent;
import ru.dorofeev.sandbox.quartzworkflow.ExecutorService.TaskCompletedEvent;
import ru.dorofeev.sandbox.quartzworkflow.QueueManager.TaskPoppedEvent;
import rx.Observable;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

import static ru.dorofeev.sandbox.quartzworkflow.ExecutorService.scheduleTaskCmd;
import static ru.dorofeev.sandbox.quartzworkflow.QueueManager.enqueueCmd;
import static ru.dorofeev.sandbox.quartzworkflow.QueueManager.giveMeMoreCmd;

class EngineImpl implements Engine {

	private Map<Class<? extends Event>, Set<String>> eventHandlers = new HashMap<>();
	private Map<String, EventHandler> eventHandlerInstances = new HashMap<>();

	private final TaskRepositoryImpl taskRepository = new TaskRepositoryImpl();

	private PublishSubject<Throwable> errors = PublishSubject.create();
	private final JobKey scheduleEventHandlersJob = new JobKey("scheduleEventHandlersJob");
	private final JobKey executeEventHandlerJob = new JobKey("executeEventHandlerJob");

	@SuppressWarnings("FieldCanBeLocal")
	private final PublishSubject<TaskRepositoryImpl.Cmd> taskRepositoryCmds = PublishSubject.create();

	@SuppressWarnings("FieldCanBeLocal")
	private final PublishSubject<QueueManager.Cmd> queueManagerCmds = PublishSubject.create();

	@SuppressWarnings("FieldCanBeLocal")
	private final PublishSubject<ExecutorService.Cmd> executorServiceCmds = PublishSubject.create();

	@SuppressWarnings("FieldCanBeLocal")
	private final QueueManager queueManager;

	@SuppressWarnings("FieldCanBeLocal")
	private final ExecutorService executorService;

	public EngineImpl(Class<? extends java.sql.Driver> sqlDriver, String dataSourceUrl) {
		prepareDatabase(dataSourceUrl);

		this.queueManager = new QueueManager("EngineQueueManager", new QueueInMemoryStore());
		this.executorService = new ExecutorService(10, 1000);

		Observable<TaskRepositoryImpl.Event> taskRepositoryOutput = taskRepositoryCmds.compose(taskRepository::bind);
		taskRepositoryOutput
			.compose(filterMapRetry(TaskRepository.Event::isAdd, this::asEnqueueCmd))
			.subscribe(queueManagerCmds);

		taskRepositoryOutput
			.compose(filterMapRetry(TaskRepository.Event::isComplete, this::asNotifyCompletedCmd))
			.subscribe(queueManagerCmds);

		Observable<QueueManager.Event> queueManagerOutput = queueManagerCmds.compose(queueManager::bind);
		queueManagerOutput
			.compose(filterMapRetry(TaskPoppedEvent.class, this::asScheduleTaskCmd))
			.subscribe(executorServiceCmds);

		rx.Observable<ExecutorService.Event> executorServiceOutput = executorServiceCmds.compose(executorService::bind);
		executorServiceOutput
			.compose(filterMapRetry(TaskCompletedEvent.class, this::asCompleteTaskCmd))
			.subscribe(taskRepositoryCmds);

		executorServiceOutput
			.compose(filterMapRetry(IdleEvent.class, this::asGiveMeMoreCmd))
			.subscribe(queueManagerCmds);
	}

	private <I, F extends I, O> Observable.Transformer<I, O> filterMapRetry(Class<F> filter, Func1<F, O> func) {
		return observable ->
			observable.ofType(filter)
				.compose(mapRetry(func));
	}

	private <I, O> Observable.Transformer<I, O> filterMapRetry(Func1<I, Boolean> filter, Func1<I, O> func) {
		return observable ->
			observable.filter(filter)
				.compose(mapRetry(func));
	}

	private <I, O> Observable.Transformer<I, O> mapRetry(Func1<I, O> func) {
		return observable ->
			observable.map(func)
				.retryWhen(errors -> errors.doOnNext(this.errors::onNext)); // publish unhandled exceptions to errors stream
	}

	private TaskRepositoryImpl.CompleteTaskCmd asCompleteTaskCmd(TaskCompletedEvent event) {
		return new TaskRepositoryImpl.CompleteTaskCmd(event.getTaskId(), event.getException());
	}

	@Override
	public rx.Observable<Throwable> errors() {
		return this.errors;
	}

	private ExecutorService.ScheduleTaskCmd asScheduleTaskCmd(TaskPoppedEvent event) {
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

	private QueueManager.NotifyCompletedCmd asNotifyCompletedCmd(TaskRepositoryImpl.Event event) {
		return new QueueManager.NotifyCompletedCmd(event.getTask().getId());
	}

	private QueueManager.EnqueueCmd asEnqueueCmd(TaskRepositoryImpl.Event event) {
		return new QueueManager.EnqueueCmd(event.getTask().getQueueName(), event.getTask().getExecutionType(), event.getTask().getId());
	}

	private QueueManager.Cmd asGiveMeMoreCmd(ExecutorService.Event event) {
		return giveMeMoreCmd();
	}

	@Override
	public TaskRepositoryImpl getTaskRepository() {
		return taskRepository;
	}

	private void prepareDatabase(String dataSourceUrl) {
		try {
			JdbcConnection h2Connection = new JdbcConnection(DriverManager.getConnection(dataSourceUrl));
			Database db = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(h2Connection);
			Liquibase liquibase = new Liquibase("engine.db.changelog.xml", new CustomClassLoaderResourceAccessor(EngineImpl.class.getClassLoader()), db);
			liquibase.update(new Contexts());
		} catch (SQLException | LiquibaseException e) {
			throw new EngineException(e);
		}
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

	void submitHandler(TaskId parentId, Event event, String handlerUri) {
		Optional<EventHandler> handlerByUriOpt = findHandlerByUri(handlerUri);
		EventHandler eventHandler = handlerByUriOpt.orElseThrow(() -> new EngineException("Handler instance for URI " + handlerUri + " not found"));

		taskRepository.addTask(parentId, executeEventHandlerJob,
			ExecuteEventHandlerJob.params(event, handlerUri),
			eventHandler.getQueueingOption(event));
	}

	Optional<EventHandler> findHandlerByUri(String handlerUri) {
		EventHandler eventHandler = eventHandlerInstances.get(handlerUri);
		return Optional.ofNullable(eventHandler);
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

	Set<String> findHandlers(Class<? extends Event> eventType) {
		Set<String> handlers = eventHandlers.get(eventType);
		if (handlers == null)
			return Collections.emptySet();
		else
			return Collections.unmodifiableSet(handlers);
	}
}
