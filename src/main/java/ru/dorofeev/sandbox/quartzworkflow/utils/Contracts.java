package ru.dorofeev.sandbox.quartzworkflow.utils;

import static java.lang.String.format;

public class Contracts {

	public static void shouldNotBeNull(Object arg, String message, Object... args) {
		if (arg == null)
			throw new IllegalArgumentException(format(message, args));
	}

	public static void shouldBe(boolean condition, String message, Object... args) {
		if (!condition)
			throw new IllegalArgumentException(format(message, args));
	}

	public static void shouldNotBe(boolean condition, String message, Object... args) {
		if (condition)
			throw new IllegalArgumentException(format(message, args));
	}

	@SuppressWarnings("unused")
	public static void mayBeNull(Object arg) {
		// just a specification
	}
}
