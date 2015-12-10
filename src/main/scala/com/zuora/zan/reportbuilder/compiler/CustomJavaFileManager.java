package com.zuora.zan.reportbuilder.compiler;

import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import java.io.IOException;

public class CustomJavaFileManager extends ForwardingJavaFileManager<JavaFileManager> {

	private CompiledCode compiledCode;
	private ParserClassLoader cl;

	public CustomJavaFileManager(JavaFileManager fileManager, CompiledCode compiledCode, ParserClassLoader cl) {
		super(fileManager);
		this.compiledCode = compiledCode;
		this.cl = cl;
		this.cl.setCode(compiledCode);
	}

	@Override
	public JavaFileObject getJavaFileForOutput(JavaFileManager.Location location, String className, JavaFileObject.Kind kind, FileObject sibling)
			throws IOException {
		return compiledCode;
	}

	@Override
	public ClassLoader getClassLoader(JavaFileManager.Location location) {
		return cl;
	}
}
