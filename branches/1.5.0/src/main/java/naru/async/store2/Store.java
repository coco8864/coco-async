package naru.async.store2;

import java.nio.ByteBuffer;

import naru.async.BufferGetter;
import naru.async.pool.PoolBase;


public class Store extends PoolBase {
	private Page topPage;
	private Page btmPage;
	@Override
	public void recycle() {
	}
	
	public synchronized void putBuffer(ByteBuffer[] buffers){
	}
	public synchronized boolean asyncBuffer(BufferGetter bufferGetter,Object userContext){
		return false;
	}
}
