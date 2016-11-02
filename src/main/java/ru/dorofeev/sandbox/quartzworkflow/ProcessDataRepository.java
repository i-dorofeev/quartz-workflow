package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.JobDataMap;

import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class ProcessDataRepository {

	private ProcessData root = new ProcessData(null, new JobDataMap());

	ProcessData addProcessData(GlobalId parentId, Supplier<ProcessData> processDataFactory) {
		if (parentId == null)
			return root.addChild(processDataFactory);
		else
			return root.findByGlobalId(parentId)
				.map(pd -> pd.addChild(processDataFactory))
				.orElseThrow(() -> new EngineException("Couldn't find parentData[id=" + parentId + "]"));
	}

	Optional<ProcessData> findProcessData(GlobalId globalId) {
		return root.findByGlobalId(globalId);
	}

	public Stream<ProcessData> traverse() {
		return root.traverse();
	}

	public Stream<ProcessData> traverse(GlobalId rootId, Predicate<? super ProcessData> predicate) {
		return root.findByGlobalId(rootId)
			.map(pd -> pd.traverse().filter(predicate))
			.orElse(Stream.empty());
	}

	public Stream<ProcessData> traverse(GlobalId rootId, ProcessData.Result result) {
		return traverse(rootId, pd -> pd.getResult().equals(result));
	}

	public Stream<ProcessData> traverseFailed() {
		return traverse()
			.filter(pd -> pd.getResult().equals(ProcessData.Result.FAILED));
	}


}
