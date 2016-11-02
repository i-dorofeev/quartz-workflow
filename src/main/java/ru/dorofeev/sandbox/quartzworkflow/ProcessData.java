package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Optional.of;
import static org.quartz.TriggerBuilder.newTrigger;

public class ProcessData {

	public static final String PROCESS_DATA_ID = "processDataId";


	public enum Result {
		CREATED, RUNNING, SUCCESS, FAILED
	}

	private transient ProcessData parent;
	private LocalId localId;
	private GlobalId globalId;

	private final Map<LocalId, ProcessData> children = new HashMap<>();
	private final JobKey jobKey;
	private final JobDataMap jobData = new JobDataMap();
	private Result result = Result.CREATED;
	private Throwable exception;

	public ProcessData(JobKey jobKey, JobDataMap jobData) {
		this.jobKey = jobKey;
		this.jobData.putAll(jobData);
	}

	public ProcessData addChild(Supplier<ProcessData> processDataFactory) {
		ProcessData processData = processDataFactory.get();
		processData.parent = this;

		ProcessData replaced;
		do {
			processData.localId = new LocalId(Integer.toString(children.size() + 1));
			replaced = children.putIfAbsent(processData.localId, processData);
		} while (replaced != null);

		processData.refreshGlobalId();
		processData.jobData.put(PROCESS_DATA_ID, processData.globalId.toString());
		return processData;
	}

	private GlobalId computeGlobalId() {
		ProcessData root = this;
		GlobalIdBuilder globalIdBuilder = new GlobalIdBuilder();
		globalIdBuilder.addParent(this.localId);
		while (root.parent != null && root.parent.localId != null) {
			root = root.parent;
			globalIdBuilder.addParent(root.localId);
		}
		return globalIdBuilder.build();
	}

	void refreshGlobalId() {
		this.globalId = computeGlobalId();
	}

	public ProcessData getRoot() {
		ProcessData root = this;
		while (root.parent != null) {
			root = root.parent;
		}
		return root;
	}

	public Optional<ProcessData> findByGlobalId(GlobalId globalId) {
		return globalId.traverse(of(this), (pdOpt, localId) -> pdOpt.flatMap(pd -> pd.findChild(localId)));
	}

	public Stream<ProcessData> traverse() {
		return Stream.concat(
			Stream.of(this),

			this.getChildren().values().stream()
				.flatMap(ProcessData::traverse)
		);
	}

	private Optional<ProcessData> findChild(LocalId id) {
		return Optional.ofNullable(children.get(id));
	}

	public Map<LocalId, ProcessData> getChildren() {
		return Collections.unmodifiableMap(children);
	}

	public LocalId getLocalId() {
		return localId;
	}

	void setLocalId(LocalId localId) {
		this.localId = localId;
	}

	public GlobalId getGlobalId() {
		return globalId;
	}

	public JobDataMap getJobData() {
		return new JobDataMap(jobData);
	}

	public JobKey getJobKey() {
		return jobKey;
	}

	public Result getResult() {
		return result;
	}

	public Throwable getException() {
		return exception;
	}

	public void recordRunning() {
		recordResult(Result.RUNNING, null);
	}

	public void recordSuccess() {
		recordResult(Result.SUCCESS, null);
	}

	public void recordFailed(Throwable ex) {
		recordResult(Result.FAILED, ex);
	}

	private void recordResult(Result res, Throwable ex) {
		result = res;
		exception = ex;
	}

	void enqueue(Scheduler scheduler) {
		Trigger trigger = newTrigger()
			.forJob(jobKey)
			.withIdentity(globalId.toString())
			.usingJobData(new JobDataMap(jobData))
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

	public String prettyPrint() {
		return JsonUtils.toPrettyJson(this);
	}

	@Override
	public String toString() {
		return "ProcessData{" + globalId + "}";
	}
}
