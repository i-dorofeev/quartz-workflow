package ru.dorofeev.sandbox.quartzworkflow.tests.utils;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import rx.Observable;
import rx.functions.Func1;

import java.util.Date;
import java.util.List;
import java.util.Optional;

import static java.lang.Math.abs;

public class Matchers {

	public static <T> org.hamcrest.Matcher<Optional<T>> present() {
		return new BaseMatcher<Optional<T>>() {
			@Override
			public boolean matches(Object item) {
				@SuppressWarnings("unchecked")
				Optional<T> optional = (Optional<T>)item;

				return optional.isPresent();
			}

			@Override
			public void describeTo(Description description) {
			}
		};
	}

	public static org.hamcrest.Matcher<Date> equalToCurrentTimeWithin(long precision) {
		return new BaseMatcher<Date>() {

			private Long currentTime;

			@Override
			public boolean matches(Object item) {
				Date date = (Date) item;
				currentTime = System.currentTimeMillis();
				long difference = date.getTime() - currentTime;
				return abs(difference) <= precision;
			}

			@Override
			public void describeTo(Description description) {
				description.appendText(currentTime.toString());
			}

			@Override
			public void describeMismatch(Object item, Description description) {
				Date date = (Date) item;
				description
					.appendText(Long.toString(date.getTime()))
					.appendText(" (").appendText(Long.toString(abs(date.getTime() - currentTime)))
					.appendText(" ms)");
			}
		};
	}

	public static org.hamcrest.Matcher<Long> equalToWithin(long expectedValue, long precision) {
		return new BaseMatcher<Long>() {

			@Override
			public boolean matches(Object item) {
				Long value = (Long) item;
				long difference = value - expectedValue;
				return abs(difference) <= precision;
			}

			@Override
			public void describeTo(Description description) {
				description.appendText(Long.toString(expectedValue));
			}

			@Override
			public void describeMismatch(Object item, Description description) {
				Long value = (Long) item;
				description
					.appendText(Long.toString(value))
					.appendText(" (").appendText(Long.toString(abs(value - expectedValue)))
					.appendText(")");
			}
		};
	}

	public static <T> org.hamcrest.Matcher<Observable<T>> hasOnlyOneItem() {
		return hasOnlyOneItem(null, item -> true);
	}

	@SuppressWarnings("WeakerAccess")
	public static <T> org.hamcrest.Matcher<Observable<T>> hasOnlyOneItem(String descr, Func1<? super T, Boolean> predicate) {
		return new BaseMatcher<Observable<T>>() {
			@Override
			public boolean matches(Object item) {
				List<T> list = getList(item);
				System.out.println(list);
				return list.size() == 1;
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("only one item");
				if (descr != null)
					description.appendText(" ").appendText(descr);
			}

			@Override
			public void describeMismatch(Object item, Description description) {
				List<T> list = getList(item);
				description.appendText(list.toString());
			}

			private List<T> getList(Object item) {
				//noinspection unchecked
				Observable<T> observable = (Observable<T>) item;
				return observable.filter(predicate).toList().toBlocking().single();
			}
		};
	}
}
