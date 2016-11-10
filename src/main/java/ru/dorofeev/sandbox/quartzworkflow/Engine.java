package ru.dorofeev.sandbox.quartzworkflow;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class Engine {

	private final Scheduler scheduler;
	private final EngineJobFactory jobFactory;
	private final EngineSchedulerListener schedulerListener;

	private Map<Class<? extends Event>, Set<String>> eventHandlers = new HashMap<>();
	private Map<String, EventHandler> eventHandlerInstances = new HashMap<>();

	private final TaskRepository taskRepository = new TaskRepository();

	private final JobKey scheduleEventHandlersJob;
	private final JobKey executeEventHandlerJob;

	public Engine(Class<? extends java.sql.Driver> sqlDriver, String dataSourceUrl) {
		try {
			prepareDatabase(dataSourceUrl);

			StdSchedulerFactory schedulerFactory = new StdSchedulerFactory(getSchedulerProperties(sqlDriver.getName(), dataSourceUrl));
			this.scheduler = schedulerFactory.getScheduler();

			this.jobFactory = new EngineJobFactory();
			this.scheduler.setJobFactory(jobFactory);

			this.schedulerListener = new EngineSchedulerListener();
			this.scheduler.getListenerManager().addSchedulerListener(schedulerListener);

			EngineJobListener jobListener = new EngineJobListener(taskRepository);
			this.scheduler.getListenerManager().addJobListener(jobListener);

			this.scheduleEventHandlersJob = createJob("scheduleEventHandlers",
				ScheduleEventHandlersJob.class, () -> new ScheduleEventHandlersJob(this));

			this.executeEventHandlerJob = createJob("executeEventHandler",
				ExecuteEventHandlerJob.class, () -> new ExecuteEventHandlerJob(this));

			QueueManager queueManager = new QueueManager(new QueueInMemoryStore());
			this.taskRepository.events()
				.map(this::toQueueManagerCmd).compose(queueManager::bindEvents)
				.map(this::toTaskId).subscribe(this::enqueue);

		} catch (SchedulerException e) {
			throw new EngineException(e);
		}
	}

	private TaskId toTaskId(QueueManager.Event event) {
		if (event instanceof QueueManager.TaskPoppedEvent)
			return ((QueueManager.TaskPoppedEvent)event).getTaskId();
		else
			throw new EngineException("Unrecognized queue manager event " + event);
	}

	private QueueManager.Cmd toQueueManagerCmd(TaskRepository.Event event) {
		if (event.getEventType() == TaskRepository.EventType.ADD)
			return new QueueManager.EnqueueCmd(event.getTask().getQueueName(), event.getTask().getExecutionType(), event.getTask().getId());
		else if (event.getEventType() == TaskRepository.EventType.COMPLETE)
			return new QueueManager.NotifyCompletedCmd(event.getTask().getQueueName(), event.getTask().getId());
		else
			throw new EngineException("Unrecognized task repository event " + event);
	}

	public TaskRepository getTaskRepository() {
		return taskRepository;
	}

	public void resetErrors() {
		schedulerListener.resetSchedulerErrors();
	}

	public void assertSuccess() {
		schedulerListener.getSchedulerErrors().forEach(e -> { throw e; });
		taskRepository.traverseFailed().forEach(t -> { throw new EngineException("Task failed: " + t.getId()); });
	}

	private void prepareDatabase(String dataSourceUrl) {
		try {
			JdbcConnection h2Connection = new JdbcConnection(DriverManager.getConnection(dataSourceUrl));
			Database db = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(h2Connection);
			Liquibase liquibase = new Liquibase("engine.db.changelog.xml", new CustomClassLoaderResourceAccessor(Engine.class.getClassLoader()), db);
			liquibase.update(new Contexts());
		} catch (SQLException | LiquibaseException e) {
			throw new EngineException(e);
		}
	}

	private Properties getSchedulerProperties(String sqlDriverName, String dataSourceUrl) {
		Properties p = new Properties();

		p.setProperty("org.quartz.scheduler.instanceName", "DefaultQuartzScheduler");
		p.setProperty("org.quartz.scheduler.rmi.export", "false");
		p.setProperty("org.quartz.scheduler.rmi.proxy", "false");
		p.setProperty("org.quartz.scheduler.wrapJobExecutionInUserTransaction", "false");
		p.setProperty("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
		p.setProperty("org.quartz.threadPool.threadCount", "10");
		p.setProperty("org.quartz.threadPool.threadPriority", "5");
		p.setProperty("org.quartz.threadPool.threadsInheritContextClassLoaderOfInitializingThread", "true");
		p.setProperty("org.quartz.jobStore.misfireThreshold", "6000");

		p.setProperty("org.quartz.jobStore.class", "org.quartz.impl.jdbcjobstore.JobStoreTX");
		p.setProperty("org.quartz.jobStore.dataSource", "ds");
		p.setProperty("org.quartz.dataSource.ds.driver", sqlDriverName);
		p.setProperty("org.quartz.dataSource.ds.URL", dataSourceUrl);


		// p.setProperty("org.quartz.jobStore.class", "org.quartz.simpl.RAMJobStore");

		return p;
	}

	public void start() {
		try {
			scheduler.start();
		} catch (SchedulerException e) {
			throw new EngineException(e);
		}
	}

	public void shutdown() {
		try {
			scheduler.shutdown(true);
		} catch (SchedulerException e) {
			throw new EngineException(e);
		}
	}

	private <T extends Job> JobKey createJob(String name, Class<T> jobType, Callable<T> jobTypeFactory) throws SchedulerException {
		jobFactory.registerFactory(jobType, jobTypeFactory);

		JobDetail jobDetail = newJob(jobType)
			.withIdentity(name)
			.storeDurably()
			.build();

		scheduler.addJob(jobDetail, /* replace */ true);

		return jobDetail.getKey();
	}

	public Task submitEvent(Event event) {
		return submitEvent(/* parentId */ null, event);
	}

	Task submitEvent(TaskId parentId, Event event) {
		return taskRepository.addTask(parentId, scheduleEventHandlersJob, ScheduleEventHandlersJob.params(event), /* queueingOption */ null);
	}

	public void retryExecution(TaskId taskId) {
		enqueue(taskId);
	}

	void submitHandler(TaskId parentId, Event event, String handlerUri) {
		Optional<EventHandler> handlerByUriOpt = findHandlerByUri(handlerUri);
		EventHandler eventHandler = handlerByUriOpt.orElseThrow(() -> new EngineException("Handler instance for URI " + handlerUri + " not found"));

		taskRepository.addTask(parentId, executeEventHandlerJob,
			ExecuteEventHandlerJob.params(event, handlerUri),
			eventHandler.getQueueingOption(event));
	}

	private void enqueue(TaskId taskId) {
		Optional<Task> taskOpt = taskRepository.findTask(taskId);
		Task t = taskOpt.orElseThrow(() -> new EngineException("Task " + taskId + " not found."));

		Trigger trigger = newTrigger()
			.forJob(t.getJobKey())
			.withIdentity(t.getId().toString())
			.usingJobData(new JobDataMap(t.getJobData()))
			.startNow()
			.build();

		scheduleTrigger(scheduler, trigger);
	}

	private void scheduleTrigger(Scheduler scheduler, Trigger trigger) {
		try {
			scheduler.scheduleJob(trigger);
		} catch (SchedulerException e) {
			throw new EngineException(e);
		}
	}

	Optional<EventHandler> findHandlerByUri(String handlerUri) {
		EventHandler eventHandler = eventHandlerInstances.get(handlerUri);
		return Optional.ofNullable(eventHandler);
	}

	public void registerEventHandlerInstance(String handlerUri, EventHandler eventHandler) {
		EventHandler replacedHandler = eventHandlerInstances.putIfAbsent(handlerUri, eventHandler);
		if (replacedHandler != null)
			throw new EngineException("Event handler " + eventHandler + " is already registered for uri " + handlerUri);
	}

	public void registerEventHandler(Class<? extends Event> eventType, String handlerUri) {
		Set<String> handlersForEventType = eventHandlers.get(eventType);
		if (handlersForEventType == null) {
			handlersForEventType = new HashSet<>();
			eventHandlers.put(eventType, handlersForEventType);
		}

		handlersForEventType.add(handlerUri);
	}

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
