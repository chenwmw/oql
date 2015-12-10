package com.zuora.zan.reportbuilder.orm.impl;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.Row;
import com.datastax.driver.mapping.annotations.Param;
import com.zuora.zan.reportbuilder.query.BeanAdapter;
import com.zuora.zan.reportbuilder.query.ParamMetadata;
import com.zuora.zan.reportbuilder.query.Query;
import com.zuora.zan.reportbuilder.query.RowAdapter;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class MethodFilter {
	private static Map<String, Query<Row>> rowBindings = new HashMap<String, Query<Row>>();
	private static Map<String, Query> beanBindings = new HashMap<String, Query>();
	
	private String[] paramNames;
	private ParamMetadata paramMetadata;
	private Method sourceMethod;
	private Map<Integer, Integer> argsIndexMapping;
	private String filtering;
	private RowAdapter rowAdapter;
	private BeanAdapter beanAdapter;

	public MethodFilter(Method sourceMethod, Method method, String filtering, Class<?> klass) {
		this.sourceMethod = sourceMethod;
		this.argsIndexMapping = sourceMethod == null ? null : parseArgsIndexMapping(sourceMethod, method);
		this.filtering = filtering;
		this.rowAdapter = new RowAdapter(klass);
		this.beanAdapter = new BeanAdapter(klass);
		parseParamNames(method);
	}

	public Method getSource() {
		return sourceMethod;
	}

	public Object[] prepareSrcArgs(Object[] args) {
		Class<?>[] types = sourceMethod.getParameterTypes();
		Object[] newArgs = new Object[types == null ? 0 : types.length];
		for (int i = 0; i < newArgs.length; i++) {
			if (argsIndexMapping == null) {
				newArgs[i] = args[i];
			} else {
				Integer idx = argsIndexMapping.get(i);
				if (idx == null)
					throw new IllegalArgumentException("Cannot find the argument at index:" + i);
				newArgs[i] = args[idx];
			}
		}
		return newArgs;
	}

	private Map<Integer, Integer> parseArgsIndexMapping(Method sourceMethod, Method method) {
		Map<String, Integer> name2IdxMapping = new HashMap<String, Integer>();
		Annotation[][] paramAnnotations = method.getParameterAnnotations();
		for (int i = 0; i < paramAnnotations.length; i++) {
			String paramName = getParamName(paramAnnotations[i]);
			if (paramName == null) {
				throw new IllegalArgumentException(String.format("To use OQL, please use @Param annotation to name your parameters", method.getName()));
			} else {
				name2IdxMapping.put(paramName, i);
			}
		}
		Map<Integer, Integer> argsIdxMapping = new HashMap<Integer, Integer>();
		paramAnnotations = sourceMethod.getParameterAnnotations();
		for (int i = 0; i < paramAnnotations.length; i++) {
			String paramName = getParamName(paramAnnotations[i]);
			if (paramName == null) {
				throw new IllegalArgumentException(String.format("To use OQL, please use @Param annotation to name your parameters", method.getName()));
			} else {
				Integer idx = name2IdxMapping.get(paramName);
				if (idx == null)
					throw new IllegalArgumentException(String.format("Cannot find %s in %s", paramName, method.getName()));
				argsIdxMapping.put(i, idx);
			}
		}
		return argsIdxMapping;
	}

	private void parseParamNames(Method method) {
		Class<?>[] paramTypes = method.getParameterTypes();
		Annotation[][] paramAnnotations = method.getParameterAnnotations();
		paramNames = new String[paramTypes.length];
		paramMetadata = new ParamMetadata();
		for (int i = 0; i < paramTypes.length; i++) {
			String paramName = getParamName(paramAnnotations[i]);
			if (paramName == null) {
				throw new IllegalArgumentException(String.format("To use OQL, please use @Param annotation to name your parameters", sourceMethod.getName()));
			} else {
				paramNames[i] = paramName;
				paramMetadata.add(paramName, paramTypes[i]);
			}
		}
	}

	private String getParamName(Annotation[] paramAnnotations) {
		for (Annotation a : paramAnnotations) {
			if (a.annotationType().equals(Param.class)) {
				return ((Param) a).value();
			}
		}
		return null;
	}

	private Map<String, Object> getParamValues(Object[] args) {
		Map<String, Object> paramValues = new HashMap<String, Object>();
		for (int i = 0; i < paramNames.length; i++) {
			if (args[i] != null && paramNames[i] != null) {
				paramValues.put(paramNames[i], args[i]);
			}
		}
		return paramValues;
	}

	public Query<Row> bindRow(Object[] args) {
		Query<Row> query = rowBindings.get(filtering);
		if (query == null) {
			query = new Query<Row>(rowAdapter, paramMetadata).filter(filtering);
			rowBindings.put(filtering, query);
		}
		return query.bind(getParamValues(args));
	}

	public Query bindBean(Object[] args) {
		Query query = beanBindings.get(filtering);
		if (query == null) {
			query = new Query(beanAdapter, paramMetadata).filter(filtering);
			beanBindings.put(filtering, query);
		}
		return query.bind(getParamValues(args));
	}
}
