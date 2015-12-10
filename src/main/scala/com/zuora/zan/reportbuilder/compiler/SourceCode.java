package com.zuora.zan.reportbuilder.compiler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;

import javax.tools.SimpleJavaFileObject;

public class SourceCode extends SimpleJavaFileObject {
	private StringBuilder contents = null;
	private String className;

	public SourceCode(String className, StringBuilder contents) {
		super(URI.create("string:///" + className.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
		this.contents = contents;
		this.className = className;
	}

	public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
		return contents;
	}

	public void save() {
		File srcFile = getSourceFile();
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(srcFile));
			writer.write(contents.toString());
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(writer != null) {
				try {
					writer.close();
				} catch (IOException e) {}
			}
		}
	}

	private String getFileDir() {
		int dot = className.lastIndexOf('.');
		if (dot != -1) 
			return className.substring(0, dot).replaceAll("\\.", "/");
		else
			return null;
	}
	private String getFileName() {
		int dot = className.lastIndexOf('.');
		if (dot != -1) 
			return className.substring(dot + 1) + ".java";
		else
			return className + ".java";
	}

	private File getSourceFile() {
		String user = System.getProperty("user.home");
		File baseDir = new File(user);
		if (!baseDir.exists()) {
			String tmp = System.getProperty("java.io.tmpdir");
			baseDir = new File(tmp);
		}
		File oqlDir = new File(baseDir, ".oql");
		if (!oqlDir.exists())
			oqlDir.mkdir();
		File srcDir = new File(oqlDir, "src");
		if (!srcDir.exists())
			srcDir.mkdir();
		String filePath = getFileDir();
		if(filePath != null) {
			srcDir = new File(srcDir, filePath);
			if(!srcDir.exists())
				srcDir.mkdirs();
		}
		String fileName = getFileName();
		File srcFile = new File(srcDir, fileName);
		return srcFile;
	}
}
