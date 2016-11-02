package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import rx.Observable;
import rx.functions.Func1;

import java.util.List;

public class Matchers {

	public static <T> org.hamcrest.Matcher<Observable<T>> hasOnlyOneItem() {
		return hasOnlyOneItem(null, pd -> true);
	}

	public static <T> org.hamcrest.Matcher<Observable<T>> hasOnlyOneItem(String descr, Func1<? super T, Boolean> predicate) {
		return new BaseMatcher<Observable<T>>() {
			@Override
			public boolean matches(Object item) {
				//noinspection unchecked
				Observable<T> observable = (Observable<T>) item;
				List<T> list = observable.filter(predicate).toList().toBlocking().single();
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
