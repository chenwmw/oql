package com.zuora.zan.reportbuilder.compiler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import javax.tools.SimpleJavaFileObject;

public class CompiledCode extends SimpleJavaFileObject {
	private ByteArrayOutputStream baos = new ByteArrayOutputStream();

	public CompiledCode(String className) {
		super(URI.create(className), Kind.CLASS);
	}

	@Override
	public OutputStream openOutputStream() throws IOException {
		return baos;
	}

	public byte[] getByteCode() {
		return baos.toByteArray();
	}
}
