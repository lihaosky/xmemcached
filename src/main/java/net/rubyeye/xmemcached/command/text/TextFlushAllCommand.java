package net.rubyeye.xmemcached.command.text;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import net.rubyeye.xmemcached.buffer.BufferAllocator;
import net.rubyeye.xmemcached.codec.MemcachedDecoder;
import net.rubyeye.xmemcached.command.FlushAllCommand;
import net.rubyeye.xmemcached.impl.MemcachedTCPSession;

public class TextFlushAllCommand extends FlushAllCommand {

	public TextFlushAllCommand(CountDownLatch latch) {
		super(latch);

	}

	@Override
	public final boolean decode(MemcachedTCPSession session, ByteBuffer buffer) {
		String line = MemcachedDecoder.nextLine(session, buffer);
		if (line != null) {
			if (line.equals("OK")) {
				setResult(Boolean.TRUE);
				countDownLatch();
				return true;
			} else
				decodeError(line);
		}
		return false;
	}

	@Override
	public final void encode(BufferAllocator bufferAllocator) {
		this.ioBuffer = bufferAllocator.wrap(FLUSH_ALL.slice());
	}

}
