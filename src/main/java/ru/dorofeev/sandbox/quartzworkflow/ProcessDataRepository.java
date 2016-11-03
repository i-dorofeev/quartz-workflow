package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.JobDataMap;
import org.quartz.JobKey;
import rx.Subscriber;
import rx.functions.Func1;

import java.util.*;
import java.util.stream.Stream;

import static java.util.Optional.ofNullable;

public class ProcessDataRepository {

	private Map<LocalId, ProcessData> processDataMap = new HashMap<>();
	private Map<LocalId, Set<LocalId>> childrenIndex = new HashMap<>();

	private void indexChild(LocalId parent, LocalId child) {
		Set<LocalId> children = childrenIndex.get(parent);
		if (children == null) {
			children = new HashSet<>();
			childrenIndex.put(parent, children);
		}

		children.add(child);
	}

	synchronized ProcessData addProcessData(LocalId parentId, JobKey jobKey, JobDataMap jobDataMap) {
		if (parentId != null && !processDataMap.containsKey(parentId))
			throw new EngineException("ProcessData[id=" + parentId + "] not found");

		LocalId localId = LocalId.createUniqueLocalId();
		while (processDataMap.containsKey(localId)) {
			localId = LocalId.createUniqueLocalId();
		}

		ProcessData pd = new ProcessData(localId, parentId, jobKey, jobDataMap);
		processDataMap.put(localId, pd);

		if (parentId != null)
			indexChild(parentId, pd.getLocalId());

		return pd;
	}

	Optional<ProcessData> findProcessData(LocalId localId) {
		return ofNullable(processDataMap.get(localId));
	}

	@SuppressWarnings("WeakerAccess")
	public Stream<ProcessData> traverse() {
		return processDataMap.values().stream();
	}

	@SuppressWarnings("WeakerAccess")
	public rx.Observable<ProcessData> traverse(LocalId rootId, Func1<? super ProcessData, Boolean> predicate) {
		return rx.Observable.<ProcessData>create(subscriber -> {
				traverse(rootId, subscriber);
				subscriber.onCompleted();
		}).filter(predicate);
	}

	private void traverse(LocalId rootId, Subscriber<? super ProcessData> subscriber) {
		ProcessData pd = processDataMap.get(rootId);
		if (pd == null)
			subscriber.onError(new EngineException("Couldn't find processData[id=" + rootId + "]"));
		else {
			subscriber.onNext(pd);
			ofNullable(childrenIndex.get(pd.getLocalId()))
				.ifPresent(children -> children.forEach(id -> traverse(id, subscriber)));
		}
	}

	public rx.Observable<ProcessData> traverse(LocalId rootId, ProcessData.Result result) {
		return traverse(rootId, processData -> processData.getResult().equals(result));
	}

	@SuppressWarnings("WeakerAccess")
	public Stream<ProcessData> traverseFailed() {
		return traverse()
			.filter(pd -> pd.getResult().equals(ProcessData.Result.FAILED));
	}


}
