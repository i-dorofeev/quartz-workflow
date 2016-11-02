package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.JobDataMap;
import org.quartz.JobKey;
import rx.functions.Func1;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Optional.ofNullable;

public class ProcessDataRepository {

	private Map<LocalId, ProcessData> processDataMap = new HashMap<>();

	synchronized ProcessData addProcessData(LocalId parentId, JobKey jobKey, JobDataMap jobDataMap) {
		if (parentId != null && !processDataMap.containsKey(parentId))
			throw new EngineException("ProcessData[id=" + parentId + "] not found");

		LocalId localId = LocalId.createUniqueLocalId();
		while (processDataMap.containsKey(localId)) {
			localId = LocalId.createUniqueLocalId();
		}

		ProcessData pd = new ProcessData(localId, parentId, jobKey, jobDataMap);
		processDataMap.put(localId, pd);
		return pd;
	}

	Optional<ProcessData> findProcessData(LocalId localId) {
		return ofNullable(processDataMap.get(localId));
	}

	public Stream<ProcessData> traverse() {
		return processDataMap.values().stream();
	}

	public rx.Observable<ProcessData> traverse(LocalId rootId, Func1<? super ProcessData, Boolean> predicate) {
		return rx.Observable.<ProcessData>create(subscriber -> {
			ProcessData pd = processDataMap.get(rootId);
			while (pd != null){
				subscriber.onNext(pd);
				pd = processDataMap.get(pd.getParentId());
			}
		}).filter(predicate);
	}

	public rx.Observable<ProcessData> traverse(LocalId rootId, ProcessData.Result result) {
		return traverse(rootId, processData -> processData.getResult().equals(result));
	}

	public Stream<ProcessData> traverseFailed() {
		return traverse()
			.filter(pd -> pd.getResult().equals(ProcessData.Result.FAILED));
	}


}
