package ru.dorofeev.sandbox.quartzworkflow.serialization;

public interface SerializedObject {

	void addString(String name, String value);
	void addUntypedObject(String name, Object obj);
	void addTypedObject(String name, Object obj);

	String build();

	String getString(String eventHandlerUri);

	<T> T getUntypedObject(String name, Class<T> type);

	<T> T getTypedObject(String name, Class<T> type);
}
