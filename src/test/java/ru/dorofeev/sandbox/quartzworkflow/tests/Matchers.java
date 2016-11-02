package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Matchers {

	public static <T> org.hamcrest.Matcher<Stream<T>> hasOnlyOneItem() {
		return hasOnlyOneItem(null, pd -> true);
	}

	public static <T> org.hamcrest.Matcher<Stream<T>> hasOnlyOneItem(String descr, Predicate<T> predicate) {
		return new BaseMatcher<Stream<T>>() {
			@Override
			public boolean matches(Object item) {
				//noinspection unchecked
				Stream<T> stream = (Stream<T>) item;
				List<T> list = stream.filter(predicate).collect(Collectors.toList());
				return list.size() == 1;
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("hasOnlyOne");
				if (descr != null)
					description.appendText(" ").appendText(descr);
			}
		};
	}
}
