package com.zuora.zan.reportbuilder.lucene;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraIndexInput extends IndexInput {
	private IOContext context;
	private Session session;
	private String dir;
	private String id;
	private String name;
	private long length;
	private long blockSize;
	private long currId;
	private long position;
	private FileBlock current;

	public CassandraIndexInput(Session session, String dirname, String id, String name, IOContext context) {
		super(dirname + "/" + name);
		this.context = context;
		this.name = name;
		this.session = session;
		this.dir = dirname;
		this.id = id;
		this.position = 0;
		loadFile();
	}

	@Override
	public IndexInput clone() {
		return new CassandraIndexInput(session, dir, id, name, context);
	}

	private void loadFile() {
		ResultSet rs = session.execute("select distinct dir, id, name, length, block_size, curr_block, created_time, "
				+ "modified_time, check_sum from zan.lucene_index where dir='" + dir + "' and id='" + id + "'");
		Row one = rs.one();
		if (one != null) {
			id = one.getString("id");
			length = one.getLong("length");
			blockSize = one.getLong("block_size");
			currId = 0;
			loadCurrBlock();
		}
	}

	private void loadCurrBlock() {
		String query = "select block_id, block from zan.lucene_index where dir='" + dir + "' and id='" + id + "' and block_id=" + currId;
		ResultSet rs = session.execute(query);
		Row one = rs.one();
		if (one != null) {
			current = new FileBlock();
			current.setBlockId(one.getLong("block_id"));
			ByteBuffer bytes = one.getBytes("block");
			if (bytes != null) {
				current.getBuffer().put(bytes);
				current.getBuffer().flip();
			}
		}
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public long getFilePointer() {
		return position;
	}

	@Override
	public void seek(long pos) throws IOException {
		if (position != pos) {
			position = pos;
			long blockId = position / blockSize;
			if (blockId != currId) {
				currId = blockId;
				loadCurrBlock();
			}
			current.getBuffer().position((int) (position - blockId * blockSize));
		}
	}

	@Override
	public long length() {
		return length;
	}

	@Override
	public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
		return new SliceIndexInput(sliceDescription, new CassandraIndexInput(session, dir, id, name, context), offset, length);
	}

	@Override
	public byte readByte() throws IOException {
		if (position >= length)
			return (byte) -1;
		if (position == (currId + 1) * blockSize) {
			currId = position / blockSize;
			loadCurrBlock();
		}
		position++;
		return current.getBuffer().get();
	}

	@Override
	public void readBytes(byte[] b, int offset, int len) throws IOException {
		int count = 0;
		while (position < length && count < len) {
			b[offset + (count++)] = readByte();
		}
	}

}
