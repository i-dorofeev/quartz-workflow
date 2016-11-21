package ru.dorofeev.sandbox.quartzworkflow.serialization;

import com.google.gson.*;

class JsonSerializedObject implements SerializedObject {

	private final static Gson gson = new Gson();
	private final static JsonParser parser = new JsonParser();

	static JsonSerializedObject parse(String src) {
		JsonObject jsonObject = parser.parse(src).getAsJsonObject();
		return new JsonSerializedObject(jsonObject);
	}

	private final JsonObject jsonObject;

	private JsonSerializedObject(JsonObject jsonObject) {
		this.jsonObject = jsonObject;
	}

	JsonSerializedObject() {
		this.jsonObject = new JsonObject();
	}

	@Override
	public void addString(String name, String value) {
		jsonObject.add(name, new JsonPrimitive(value));
	}

	@Override
	public void addUntypedObject(String name, Object obj) {
		JsonObject jo = new JsonObject();
		jo.add("type", new JsonPrimitive(obj.getClass().getName()));
		jo.add("value", gson.toJsonTree(obj));

		this.jsonObject.add(name, jo);
	}

	@Override
	public String build() {
		return jsonObject.toString();
	}

	@Override
	public String getString(String name) {
		return jsonObject.getAsJsonPrimitive(name).getAsString();
	}

	@Override
	public <T> T getUntypedObject(String name, Class<T> type) {
		try {
			JsonObject jo = jsonObject.getAsJsonObject(name);

			String typeStr = jo.getAsJsonPrimitive("type").getAsString();
			JsonObject valueJo = jo.getAsJsonObject("value");

			Class<?> klass = Class.forName(typeStr);
			return (T)gson.fromJson(valueJo, klass);
		} catch (ClassNotFoundException e) {
			throw new SerializerException(e);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		JsonSerializedObject that = (JsonSerializedObject) o;

		return jsonObject != null ? jsonObject.equals(that.jsonObject) : that.jsonObject == null;

	}

	@Override
	public int hashCode() {
		return jsonObject != null ? jsonObject.hashCode() : 0;
	}

	@Override
	public String toString() {
		return "JsonSerializedObject{" +
			"jsonObject=" + jsonObject +
			'}';
	}
}
