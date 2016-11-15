package ru.dorofeev.sandbox.quartzworkflow;

import java.util.HashMap;
import java.util.Map;

public class JobDataMap {

	private final Map<String, String> data = new HashMap<>();

	public void put(String param, String value) {
		data.put(param, value);
	}

	public String get(String param) {
		return data.get(param);
	}

	public void putAll(JobDataMap jobData) {
		data.putAll(jobData.data);
	}
}
