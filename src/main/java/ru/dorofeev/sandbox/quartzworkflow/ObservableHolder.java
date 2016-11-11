package ru.dorofeev.sandbox.quartzworkflow;

import rx.Observable;
import rx.Observer;
import rx.Subscriber;

import java.util.ArrayList;
import java.util.List;

public class ObservableHolder<T> {

	private final Observable<T> observable;
	private final List<Subscriber<? super T>> subscriberList;

	public ObservableHolder() {
		this.subscriberList = new ArrayList<>();
		this.observable = Observable.create(subscriberList::add);
	}

	public Observable<T> getObservable() {
		return observable;
	}

	public void onNext(T item) {
		subscriberList.forEach(s -> s.onNext(item));
	}

	@SuppressWarnings("unused")
	public void onCompleted() {
		subscriberList.forEach(Observer::onCompleted);
	}

	@SuppressWarnings("unused")
	public void onError(Throwable e) {
		subscriberList.forEach(s -> s.onError(e));
	}

	public Observer<T> nextObserver() {
		return new Observer<T>() {
			@Override public void onCompleted() { }
			@Override public void onError(Throwable e) { }

			@Override
			public void onNext(T t) {
				ObservableHolder.this.onNext(t);
			}
		};
	}
}
