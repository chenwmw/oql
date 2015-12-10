package com.zuora.zan.reportbuilder.stream;

import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class BeanSortAdapter<T> implements SortAdapter<T> {
	private static Map<Class, Comparator> comparators = new HashMap<Class, Comparator>();
	static {
		comparators.put(Comparable.class, new Comparator<Comparable>() {
			public int compare(Comparable o1, Comparable o2) {
				return o1 == null ? (o2 == null ? 0 : -1) : (o2 == null ? 1 : o1.compareTo(o2));
			}
		});
	}
	private Class<T> beanClass;

	public BeanSortAdapter(Class<T> beanClass) {
		this.beanClass = beanClass;
	}
	
	private static Field getField(String name, Class beanClass) {
		try {
			if (beanClass != null) {
				Field field = beanClass.getDeclaredField(name);
				if (field != null)
					return field;
			}
		} catch (NoSuchFieldException e) {
			if (beanClass != Object.class)
				return getField(name, beanClass.getSuperclass());
		}
		return null;
	}

	private static Comparator getFieldComparator(String name, Class beanClass) {
		Field field = getField(name, beanClass);
		return field == null ? null : getClassComparator(field.getType());
	}

	private static Comparator getClassComparator(Class clazz) {
		Comparator comparator = comparators.get(clazz);
		if (comparator == null) {
			Class matchedClass = null;
			for (Map.Entry<Class, Comparator> entry : comparators.entrySet()) {
				Class classKey = entry.getKey();
				if (classKey.isAssignableFrom(clazz)) {
					if (matchedClass == null) {
						matchedClass = classKey;
					} else if (matchedClass.isAssignableFrom(classKey) && !classKey.isAssignableFrom(matchedClass)) {
						matchedClass = classKey;
					}
				}
			}
			if (matchedClass != null) {
				comparator = comparators.get(matchedClass);
				comparators.put(matchedClass, comparator);
			}
		}
		return comparator;
	}

	@Override
	public Comparator getComparator(String name) {
		return getFieldComparator(name, beanClass);
	}

	@Override
	public Object getValue(T o, String name) {
		return getFieldValue(o, name, beanClass);
	}

	private static <T> Object getFieldValue(T o, String name, Class<?> beanClass) {
		Field field = getField(name, beanClass);
		if (field != null) {
			try {
				field.setAccessible(true);
				return field.get(o);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}
}
