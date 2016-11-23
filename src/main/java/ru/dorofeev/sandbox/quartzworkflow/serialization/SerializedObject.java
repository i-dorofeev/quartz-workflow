package ru.dorofeev.sandbox.quartzworkflow.serialization;

public interface SerializedObject {

	void addString(String name, String value);
	String getString(String eventHandlerUri);

	void addUntypedObject(String name, Object obj);
	<T> T getUntypedObject(String name, @SuppressWarnings("unused") Class<T> type);

	String build();
}
