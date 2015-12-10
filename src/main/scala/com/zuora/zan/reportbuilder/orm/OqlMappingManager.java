package com.zuora.zan.reportbuilder.orm;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Query;
import com.zuora.zan.reportbuilder.orm.impl.AccessorHandler;
import com.zuora.zan.reportbuilder.orm.impl.MethodFilter;

/**
 * OqlMappingManager extends MappingManager to provide enhanced filtering
 * ability by utilizing the OQL library written in Scala
 *
 */
public class OqlMappingManager extends MappingManager {
	public OqlMappingManager(Session session) {
		super(session);
	}

	private <T> Map<String, Method> parseQueries(Class<T> klass) {
		Map<String, Method> queries = new HashMap<String, Method>();
		Method[] methods = klass.getDeclaredMethods();
		for (Method method : methods) {
			Class<?> returnType = method.getReturnType();
			if (Result.class.isAssignableFrom(returnType)) {
				Query query = method.getAnnotation(Query.class);
				if (query != null) {
					queries.put(method.getName(), method);
				}
			}
		}
		return queries;
	}

	private <T> Map<String, MethodFilter> parseFilters(Class<T> klass) {
		Map<String, Method> queries = parseQueries(klass);
		Map<String, MethodFilter> filters = new HashMap<String, MethodFilter>();
		Method[] methods = klass.getDeclaredMethods();
		for (Method method : methods) {
			Class<?> returnType = method.getReturnType();
			if (Iterable.class.isAssignableFrom(returnType)) {
				MethodFilter filter = createMethodFilter(queries, method);
				if (filter != null) {
					filters.put(method.toString(), filter);
				}
			}
		}
		return filters;
	}

	@SuppressWarnings({ "rawtypes" })
	private MethodFilter createMethodFilter(Map<String, Method> queries, Method method) {
		Filter filter = method.getAnnotation(Filter.class);
		if (filter != null) {
			Type grt = method.getGenericReturnType();
			if (grt instanceof ParameterizedType) {
				ParameterizedType prt = (ParameterizedType) grt;
				Type[] typeArgs = prt.getActualTypeArguments();
				if (typeArgs != null && typeArgs.length > 0) {
					Type type = typeArgs[0];
					String filtering = filter.value();
					Source source = method.getAnnotation(Source.class);
					Method sourceMethod = null;
					if (source != null) {
						sourceMethod = queries.get(source.value());
						if (sourceMethod == null)
							throw new IllegalArgumentException("No such a query method named:" + source);
					}
					return new MethodFilter(sourceMethod, method, filtering, (Class) type);
				}
			}
		}
		return null;
	}

	@SuppressWarnings({ "unchecked" })
	@Override
	public <T> T createAccessor(Class<T> klass) {
		T accessor = super.createAccessor(klass);
		return (T) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[] { klass }, new AccessorHandler<T>(accessor, parseFilters(klass)));
	}
}
