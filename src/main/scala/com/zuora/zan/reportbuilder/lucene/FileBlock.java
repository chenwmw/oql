package com.zuora.zan.reportbuilder.lucene;

import java.nio.ByteBuffer;

public class FileBlock {
	public static final int DEFAULT_BLOCK_SIZE = 10 * 1024;
	private long blockId = 0;
	private ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_BLOCK_SIZE);

	public FileBlock() {
	}

	public FileBlock(long blockId, ByteBuffer buffer) {
		this.blockId = blockId;
		this.buffer = buffer;
	}

	public long getBlockId() {
		return blockId;
	}

	public void setBlockId(long blockId) {
		this.blockId = blockId;
	}

	public ByteBuffer getBuffer() {
		return buffer;
	}

	public void setBuffer(ByteBuffer buffer) {
		this.buffer = buffer;
	}
}
