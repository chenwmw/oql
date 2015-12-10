package com.zuora.zan.reportbuilder.lucene;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;

public class SliceIndexInput extends IndexInput {
	private CassandraIndexInput parent;
	private long offset;
	private long length;

	public SliceIndexInput(String resourceDescription, CassandraIndexInput parent, long offset, long length) {
		super(resourceDescription);
		this.offset = offset;
		this.parent = parent;
		if (length + offset > parent.length()) {
			this.length = parent.length() - offset;
		} else {
			this.length = length;
		}
		try {
			parent.seek(offset);
		} catch (Exception e) {
		}
	}

	@Override
	public IndexInput clone() {
		return new SliceIndexInput(toString(), (CassandraIndexInput) parent.clone(), offset, length);
	}

	@Override
	public void close() throws IOException {
		parent.close();
	}

	@Override
	public long getFilePointer() {
		return parent.getFilePointer() - offset;
	}

	@Override
	public void seek(long pos) throws IOException {
		parent.seek(offset + pos);
	}

	@Override
	public long length() {
		return length;
	}

	@Override
	public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
		return null;
	}

	@Override
	public byte readByte() throws IOException {
		return parent.readByte();
	}

	@Override
	public void readBytes(byte[] b, int offset, int len) throws IOException {
		parent.readBytes(b, offset, len);
	}

}
