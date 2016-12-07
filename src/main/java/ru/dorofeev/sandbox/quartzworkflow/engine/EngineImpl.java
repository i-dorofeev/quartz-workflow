package ru.dorofeev.sandbox.quartzworkflow.engine;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.execution.Executable;
import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService;
import ru.dorofeev.sandbox.quartzworkflow.jobs.Job;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobRepository;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions;
import ru.dorofeev.sandbox.quartzworkflow.utils.ErrorObservable;
import rx.Observable;
import rx.subjects.PublishSubject;

import java.util.*;

import static ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService.scheduleJobCmd;
import static ru.dorofeev.sandbox.quartzworkflow.jobs.JobRepository.addJobCmd;
import static ru.dorofeev.sandbox.quartzworkflow.jobs.JobRepository.completeJobCmd;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager.*;

class EngineImpl implements Engine {

	private final Map<Class<? extends Event>, Set<String>> eventHandlers = new HashMap<>();
	private final Map<String, EventHandler> eventHandlerInstances = new HashMap<>();

	private final JobRepository jobRepository;
	private final ExecutorService executorService;

	private final ErrorObservable errors = new ErrorObservable();

	@SuppressWarnings("FieldCanBeLocal")
	private final PublishSubject<JobRepository.Cmd> jobRepositoryCmds = PublishSubject.create();

	@SuppressWarnings("FieldCanBeLocal")
	private final PublishSubject<QueueManager.Cmd> queueManagerCmds = PublishSubject.create();

	@SuppressWarnings("FieldCanBeLocal")
	private final PublishSubject<ExecutorService.Cmd> executorServiceCmds = PublishSubject.create();


	EngineImpl(JobRepository jobRepository, ExecutorService executorService, QueueManager queueManager) {
		this.jobRepository = jobRepository;
		this.executorService = executorService;

		this.errors.subscribeTo(jobRepository.getErrors());
		this.errors.subscribeTo(executorService.getErrors());
		this.errors.subscribeTo(queueManager.getErrors());

		// the data flow configuration is showed in the diagram
		// https://docs.google.com/drawings/d/1U-37n2b_m1ZcEjf91W21Ws5t4BuSQXSkVHGSO7rL0MQ/edit?usp=sharing

		Observable<JobRepository.Event> jobRepositoryOutput = jobRepositoryCmds.compose(this.jobRepository::bind);
		jobRepositoryOutput
			.compose(errors.filterMapRetry(JobRepository.JobAddedEvent.class, this::asEnqueueCmd))
			.subscribe(queueManagerCmds);

		jobRepositoryOutput
			.compose(errors.filterMapRetry(JobRepository.JobCompletedEvent.class, this::asNotifyCompletedCmd))
			.subscribe(queueManagerCmds);

		Observable<QueueManager.Event> queueManagerOutput = queueManagerCmds.compose(queueManager::bind);
		queueManagerOutput
			.compose(errors.filterMapRetry(QueueManager.JobPoppedEvent.class, this::asScheduleJobCmd))
			.subscribe(executorServiceCmds);

		rx.Observable<ExecutorService.Event> executorServiceOutput = executorServiceCmds.compose(executorService::bind);
		executorServiceOutput
			.compose(errors.filterMapRetry(ExecutorService.JobCompletedEvent.class, this::asCompleteJobCmd))
			.subscribe(jobRepositoryCmds);

		executorServiceOutput
			.compose(errors.filterMapRetry(ExecutorService.IdleEvent.class, e -> giveMeMoreCmd()))
			.subscribe(queueManagerCmds);
	}

	private JobRepository.CompleteJobCmd asCompleteJobCmd(ExecutorService.JobCompletedEvent event) {
		return completeJobCmd(event.getJobId(), event.getException(), event.getExecutionDuration(), event.getCompleted());
	}

	private ExecutorService.ScheduleJobCmd asScheduleJobCmd(QueueManager.JobPoppedEvent event) {
		JobId jobId = event.getJobId();

		Job job = jobRepository.findJob(jobId).orElseThrow(() -> new EngineException("Couldn't find job " + jobId));
		Executable executable = getExecutable(job);
		return scheduleJobCmd(jobId, job.getArgs(), executable);
	}

	private Executable getExecutable(Job job) {
		if (job.getJobKey().equals(SCHEDULE_EVENT_HANDLERS_JOB)) {
			return new ScheduleEventHandlersJob(this);
		} else if (job.getJobKey().equals(EXECUTE_EVENT_HANDLER_JOB)) {
			return new ExecuteEventHandlerJob(this);
		} else {
			throw new EngineException("Unknown job key " + job.getJobKey());
		}
	}

	private QueueManager.NotifyCompletedCmd asNotifyCompletedCmd(JobRepository.JobCompletedEvent event) {
		return notifyCompletedCmd(event.getJobId());
	}

	private QueueManager.EnqueueCmd asEnqueueCmd(JobRepository.JobAddedEvent event) {
		return enqueueCmd(event.getJob().getQueueName(), event.getJob().getExecutionType(), event.getJob().getId(), event.getJob().getTargetNodeSpecification());
	}

	@Override
	public rx.Observable<Throwable> errors() {
		return this.errors.asObservable();
	}

	public JobRepository getJobRepository() {
		return jobRepository;
	}

	@Override
	public void start() {
		executorService.start();
	}

	@Override
	public void shutdown() {
		executorService.shutdown();
	}

	@Override
	public Job submitEvent(Event event) {
		return submitEvent(/* parentId */ null, event);
	}

	Job submitEvent(JobId parentId, Event event) {
		return jobRepository.addJob(
			parentId, SCHEDULE_EVENT_HANDLERS_JOB,
			new ScheduleEventHandlersJob.Args(event), QueueingOptions.DEFAULT);
	}

	@Override
	public void retryExecution(JobId jobId) {
		Optional<Job> jobOptional = jobRepository.findJob(jobId);

		Job job = jobOptional.orElseThrow(() -> new EngineException("Job " + jobId + " not found"));
		queueManagerCmds.onNext(enqueueCmd(job.getQueueName(), job.getExecutionType(), job.getId(), job.getTargetNodeSpecification()));
	}

	@Override
	public void registerEventHandlerInstance(String handlerUri, EventHandler eventHandler) {
		EventHandler replacedHandler = eventHandlerInstances.putIfAbsent(handlerUri, eventHandler);
		if (replacedHandler != null)
			throw new EngineException("Event handler " + eventHandler + " is already registered for uri " + handlerUri);
	}

	@Override
	public void registerEventHandler(Class<? extends Event> eventType, String handlerUri) {
		eventHandlers.computeIfAbsent(eventType, k -> new HashSet<>()).add(handlerUri);
	}

	@Override
	public void registerEventHandler(Class<? extends Event> cmdEventType, EventHandler cmdHandler, String handlerUri) {
		registerEventHandlerInstance(handlerUri, cmdHandler);
		registerEventHandler(cmdEventType, handlerUri);
	}

	void submitHandler(JobId parentId, Event event, String handlerUri) {
		Optional<EventHandler> handlerByUriOpt = findHandlerByUri(handlerUri);
		EventHandler eventHandler = handlerByUriOpt.orElseThrow(() -> new EngineException("Handler instance for URI " + handlerUri + " not found"));

		jobRepositoryCmds.onNext(
			addJobCmd(
				parentId, EXECUTE_EVENT_HANDLER_JOB,
				new ExecuteEventHandlerJob.Args(handlerUri, event),
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
