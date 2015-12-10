package com.zuora.zan.reportbuilder.lucene;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.zuora.zan.reportbuilder.stream.Function1;
import com.zuora.zan.reportbuilder.stream.Predicate;
import com.zuora.zan.reportbuilder.stream.Stream;

public class LuceneDirectory extends Directory {
	private Map<String, CassandraIndexOutput> outputs = new HashMap<String, CassandraIndexOutput>();
	private Session session;
	private String dirname;

	public LuceneDirectory(Session session, String dirname) {
		this.session = session;
		this.dirname = dirname;
	}

	private Stream<Row> getFiles() {
		ResultSet rs = session.execute("select * from zan.lucene_index");
		return Stream.toStream(rs).filter(new Predicate<Row>() {
			@Override
			public boolean apply(Row o) {
				return dirname.equals(o.getString("dir"));
			}
		});
	}

	private Row getFile(final String name) {
		ResultSet rs = session.execute("select * from zan.lucene_index");
		return Stream.toStream(rs).filter(new Predicate<Row>() {
			@Override
			public boolean apply(Row o) {
				return dirname.equals(o.getString("dir")) && name.equals(o.getString("name"));
			}
		}).one();
	}

	private String getId(String name) {
		Row file = getFile(name);
		return file == null ? null : file.getString("id");
	}

	@Override
	public String[] listAll() throws IOException {
		List<String> fileList = getFiles().map(new Function1<String, Row>() {
			@Override
			public String apply(Row o) {
				return o.getString("name");
			}
		}).toList();
		return fileList.toArray(new String[fileList.size()]);
	}

	@Override
	public void deleteFile(String name) throws IOException {
		String id = getId(name);
		if (id != null) {
			session.execute("delete from zan.lucene_index where dir='" + dirname + "' and id='" + id + "'");
		}
	}

	@Override
	public long fileLength(String name) throws IOException {
		String id = getId(name);
		if (id != null) {
			ResultSet rs = session.execute("select length from zan.lucene_index where dir='" + dirname + "' and id='" + id + "'");
			Row one = rs.one();
			if (one != null) {
				return one.getLong("length");
			}
		}
		return -1;
	}

	@Override
	public IndexOutput createOutput(String name, IOContext context) throws IOException {
		String id = getId(name);
		CassandraIndexOutput output = new CassandraIndexOutput(session, dirname, name, id, context);
		outputs.put(name, output);
		return output;
	}

	@Override
	public void sync(Collection<String> names) throws IOException {
		for (String name : names) {
			CassandraIndexOutput indexOutput = outputs.get(name);
			if (indexOutput != null) {
				indexOutput.flush();
			}
		}
	}

	@Override
	public void renameFile(String source, String dest) throws IOException {
		String id = getId(source);
		if (id != null) {
			session.execute("update zan.lucene_index set name = '" + dest + "' where dir = '" + dirname + "' and id = '" + id + "'");
		}
	}

	@Override
	public IndexInput openInput(String name, IOContext context) throws IOException {
		String id = getId(name);
		if (id == null)
			throw new FileNotFoundException(name);
		CassandraIndexInput input = new CassandraIndexInput(session, dirname, id, name, context);
		return input;
	}

	@Override
	public Lock makeLock(String name) {
		return new Lock() {
			private boolean locked = false;

			@Override
			public boolean obtain() throws IOException {
				locked = true;
				return true;
			}

			@Override
			public void close() throws IOException {
				locked = false;
			}

			@Override
			public boolean isLocked() throws IOException {
				return locked;
			}
		};
	}

	@Override
	public void close() throws IOException {
	}

}
