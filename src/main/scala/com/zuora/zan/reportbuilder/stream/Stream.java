package com.zuora.zan.reportbuilder.stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Stream<T> implements Iterable<T> {
	private Iterable<T> iterable;

	private Stream(Iterable<T> iterable) {
		this.iterable = iterable;
	}

	public static Stream<Integer> times(int maximum) {
		return range(0, maximum);
	}

	public static Stream<Integer> range(final int start, final int end) {
		return new Stream<Integer>(new Iterable<Integer>() {
			@Override
			public Iterator<Integer> iterator() {
				return new Iterator<Integer>() {
					int current = start;

					@Override
					public boolean hasNext() {
						return current < end;
					}

					@Override
					public Integer next() {
						return current++;
					}

					@Override
					public void remove() {
					}
				};
			}
		});
	}

	public static <T> Stream<T> toStream(Iterable<T> iterable) {
		return new Stream<T>(iterable);
	}

	public static <T> Stream<T> toStream(T[] array) {
		return toStream(Arrays.asList(array));
	}

	public T one() {
		Iterator<T> iterator = iterator();
		if (iterator.hasNext())
			return iterator.next();
		return null;
	}

	public long average(final Action<T> action) {
		long now = System.currentTimeMillis();
		int times = count(new Predicate<T>() {
			@Override
			public boolean apply(T o) {
				action.apply(o);
				return true;
			}
		});
		long consumed = System.currentTimeMillis() - now;
		return (long) ((double) consumed / times);
	}

	public void foreach(Action<T> action) {
		for (T o : iterable) {
			action.apply(o);
		}
	}

	public boolean forall(Predicate<T> predicate) {
		for (T o : iterable) {
			if (!predicate.apply(o)) {
				return false;
			}
		}
		return true;
	}

	public <R> Stream<R> map(final Function1<R, T> mapper) {
		return new Stream<R>(new Iterable<R>() {
			@Override
			public Iterator<R> iterator() {
				final Iterator<T> src = iterable.iterator();
				return new Iterator<R>() {
					@Override
					public boolean hasNext() {
						return src.hasNext();
					}

					@Override
					public R next() {
						return mapper.apply(src.next());
					}

					@Override
					public void remove() {
						src.remove();
					}
				};
			}
		});
	}

	public Iterable<T> toIterable() {
		return iterable;
	}

	@Override
	public Iterator<T> iterator() {
		return iterable.iterator();
	}

	public <R> Stream<R> flatMap(final Function1<Stream<R>, T> mapper) {
		return new Stream<R>(new Iterable<R>() {
			final Iterator<T> src = iterable.iterator();

			@Override
			public Iterator<R> iterator() {
				return new Iterator<R>() {
					Iterator<R> iter;
					R current;

					private R findNext() {
						if (iter != null && iter.hasNext()) {
							return iter.next();
						}
						while (src.hasNext()) {
							iter = mapper.apply(src.next()).iterator();
							if (iter.hasNext())
								return iter.next();
						}
						return null;
					}

					@Override
					public boolean hasNext() {
						current = findNext();
						return current != null;
					}

					@Override
					public R next() {
						return current;
					}

					@Override
					public void remove() {
					}
				};
			}
		});
	}

	public Stream<T> filter(final Predicate<T> predicate) {
		return new Stream<T>(new Iterable<T>() {
			@Override
			public Iterator<T> iterator() {
				return new FilterIterator<T>(iterable.iterator(), predicate);
			}
		});
	}

	static class NegativePredicate<T> implements Predicate<T> {
		private Predicate<T> predicate;

		public NegativePredicate(Predicate<T> p) {
			predicate = p;
		}

		@Override
		public boolean apply(T o) {
			return !predicate.apply(o);
		}

	}

	public Stream<T> remove(Predicate<T> predicate) {
		return filter(new NegativePredicate<T>(predicate));
	}

	public boolean exists(Predicate<T> predicate) {
		for (T o : iterable) {
			if (predicate.apply(o))
				return true;
		}
		return false;
	}

	public Tuple2<Stream<T>, Stream<T>> partition(Predicate<T> predicate) {
		return new Tuple2<Stream<T>, Stream<T>>(filter(predicate), remove(predicate));
	}

	public <K> Map<K, List<T>> groupBy(Function1<K, T> f) {
		Map<K, List<T>> map = new HashMap<K, List<T>>();
		for (T o : iterable) {
			K key = f.apply(o);
			if (key != null) {
				List<T> value = map.get(key);
				if (value == null) {
					value = new LinkedList<T>();
					map.put(key, value);
				}
				value.add(o);
			}
		}
		return map;
	}

	public int count(Predicate<T> predicate) {
		int count = 0;
		for (T o : iterable) {
			if (predicate.apply(o))
				count++;
		}
		return count;
	}

	public T find(Predicate<T> predicate) {
		for (T o : iterable) {
			if (predicate.apply(o))
				return o;
		}
		return null;
	}

	public Stream<T> reverse(Iterable<T> iterable) {
		LinkedList<T> result = new LinkedList<T>();
		for (T o : iterable)
			result.addFirst(o);
		return new Stream<T>(result);
	}

	public List<T> toList() {
		return collect(new ArrayList<T>(), new Function2<List<T>, List<T>, T>() {
			@Override
			public List<T> apply(List<T> o1, T o2) {
				o1.add(o2);
				return o1;
			}
		});
	}

	static class Idempotent<R> implements Function1<R, R> {
		@Override
		public R apply(R o) {
			return o;
		}
	}

	public T collect(final Function2<T, T, T> add) {
		return collect((T) null, new Function2<T, T, T>() {
			@Override
			public T apply(T o1, T o2) {
				if (o1 == null)
					return o2;
				return add.apply(o1, o2);
			}
		});
	}

	public <R> R collect(final R zero, Function2<R, R, T> add) {
		return collect(new Function<R>() {
			@Override
			public R apply() {
				return zero;
			}
		}, add);
	}

	public <R> R collect(Function<R> init, Function2<R, R, T> add) {
		return collect(init, add, new Idempotent<R>());
	}

	public <R, S> R collect(Function<S> init, Function2<S, S, T> add, Function1<R, S> complete) {
		S zero = init.apply();
		for (T o : iterable) {
			zero = add.apply(zero, o);
		}
		return complete.apply(zero);
	}

	@SuppressWarnings("unchecked")
	public static <T> List<T> pageList(Paging paging, List<T> list) {
		int startIndex = paging.getPageNo() * paging.getPageSize();
		int endIndex = startIndex + paging.getPageSize();
		if (startIndex >= list.size())
			return Collections.EMPTY_LIST;
		if (endIndex > list.size())
			endIndex = list.size();
		return list.subList(startIndex, endIndex);
	}
}
