package com.zuora.zan.reportbuilder.compiler;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

public class ParserClassLoader extends ClassLoader {
	private static final boolean DEBUG = true;
	private Map<String, CompiledCode> customCompiledCode = new HashMap<>();

	public ParserClassLoader(ClassLoader parent) {
		super(parent);
	}

	public void setCode(CompiledCode cc) {
		customCompiledCode.put(cc.getName(), cc);
	}

	@Override
	protected Class<?> findClass(String name) throws ClassNotFoundException {
		CompiledCode cc = customCompiledCode.get(name);
		if (cc == null) {
			return super.findClass(name);
		}
		byte[] byteCode = cc.getByteCode();
		return defineClass(name, byteCode, 0, byteCode.length);
	}

	public static Class<?> compile(String className, StringBuilder sourceCodeInText) throws ClassNotFoundException {
		JavaCompiler javac = ToolProvider.getSystemJavaCompiler();
		SourceCode sourceCode = new SourceCode(className, sourceCodeInText);
		if(DEBUG) sourceCode.save();
		CompiledCode compiledCode = new CompiledCode(className);
		Iterable<? extends JavaFileObject> compilationUnits = Arrays.asList(sourceCode);
		ParserClassLoader cl = new ParserClassLoader(ClassLoader.getSystemClassLoader());
		CustomJavaFileManager fileManager = new CustomJavaFileManager(javac.getStandardFileManager(null, null, null), compiledCode, cl);
		JavaCompiler.CompilationTask task = javac.getTask(null, fileManager, null, null, null, compilationUnits);
		boolean result = task.call();
		return result ? cl.loadClass(className) : null;
	}

}
