package com.zuora.zan.reportbuilder.lucene;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.zip.CRC32;

import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraIndexOutput extends IndexOutput {
	private static String WRITE_STATEMENT = "insert into zan.lucene_index(dir, id, name, length, block_size, curr_block, created_time, modified_time, check_sum, block_id, block) "
			+ "values(?,?,?,?,?,?,?,?,?,?,?);";
	private Session session;
	private String dir;
	private String name;
	private String id;
	private long length;
	private long blockSize;
	private long created;
	private long modified;
	private long currBlock;
	private long checkSum;
	private BufferedChecksum bcs;
	private List<FileBlock> blocks;
	private FileBlock current;
	private BoundStatement stmt;

	public CassandraIndexOutput(Session session, String dirname, String name, String id, IOContext context) {
		super(dirname + "/" + name);
		this.session = session;
		this.dir = dirname;
		this.name = name;
		this.id = id;
		if (this.id == null) {
			createFile();
		} else {
			loadFile();
		}
		stmt = session.prepare(WRITE_STATEMENT).bind();
	}

	public void flush() {
		checkSum = bcs.getValue();
		for (FileBlock block : blocks) {
			block.getBuffer().flip();
			modified = System.currentTimeMillis();
			stmt.enableTracing();
			stmt.setString("dir", dir);
			stmt.setString("id", id);
			stmt.setString("name", name);
			stmt.setLong("length", length);
			stmt.setLong("block_size", blockSize);
			stmt.setLong("curr_block", currBlock);
			stmt.setDate("created_time", new Date(created));
			stmt.setDate("modified_time", new Date(modified));
			stmt.setLong("check_sum", checkSum);
			stmt.setLong("block_id", block.getBlockId());
			stmt.setBytes("block", block.getBuffer());
			session.execute(stmt);
		}
		blocks = new ArrayList<FileBlock>();
		loadCurrentBlock();
	}

	private void loadCurrentBlock() {
		ResultSet rs = session.execute("select block_id, block from zan.lucene_index where dir='" + dir + "' and id='" + id + "' and block_id=" + currBlock);
		Row one = rs.one();
		if (one != null) {
			current = new FileBlock();
			current.setBlockId(one.getLong("block_id"));
			ByteBuffer bytes = one.getBytes("block");
			if (bytes != null) {
				current.getBuffer().put(bytes);
			}
			blocks.add(current);
		}
	}

	private void loadFile() {
		ResultSet rs = session.execute("select distinct dir, id, name, length, block_size, curr_block, created_time, "
				+ "modified_time, check_sum, from zan.lucene_index where dir='" + dir + "' and id='" + id + "'");
		Row one = rs.one();
		if (one != null) {
			id = one.getString("id");
			length = one.getLong("length");
			blockSize = one.getLong("block_size");
			created = one.getDate("created_time").getTime();
			modified = one.getDate("modified_time").getTime();
			currBlock = one.getLong("curr_block");
			checkSum = one.getLong("check_sum");
			bcs = new BufferedChecksum(new CRC32());
			bcs.update((int) (0xffffffffL & checkSum));
			this.blocks = new ArrayList<FileBlock>();
			loadCurrentBlock();
		} else {
			createFile();
		}
	}

	private void createFile() {
		length = 0;
		blockSize = FileBlock.DEFAULT_BLOCK_SIZE;
		created = System.currentTimeMillis();
		modified = created;
		currBlock = 0;
		checkSum = 0;
		bcs = new BufferedChecksum(new CRC32());
		id = UUID.randomUUID().toString();
		current = new FileBlock();
		this.blocks = new ArrayList<FileBlock>();
		blocks.add(current);
		session.execute("insert into zan.lucene_index(dir, id, name, length, block_size, curr_block, created_time, modified_time, check_sum, block_id) "
				+ "values('" + dir + "','" + id + "','" + name + "'," + length + "," + blockSize + "," + currBlock + "," + created + "," + modified + ","
				+ checkSum + "," + currBlock + ")");
	}

	@Override
	public void close() throws IOException {
		flush();
	}

	@Override
	public long getFilePointer() {
		return length;
	}

	@Override
	public long getChecksum() throws IOException {
		checkSum = bcs.getValue();
		return checkSum;
	}

	@Override
	public void writeByte(byte b) throws IOException {
		if (current.getBuffer().hasRemaining()) {
			current.getBuffer().put(b);
		} else {
			current = new FileBlock();
			current.setBlockId(++currBlock);
			current.getBuffer().put(b);
			blocks.add(current);
		}
		length++;
		bcs.update(b);
	}

	@Override
	public void writeBytes(byte[] b, int offset, int length) throws IOException {
		if (length > 0) {
			for (int i = offset; i < offset + length; i++) {
				writeByte(b[i]);
			}
		}
	}

}
