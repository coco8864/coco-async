package naru.async.pool;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;

import naru.async.pool.Pool;
import naru.async.pool.PoolManager;
import naru.queuelet.test.TestBase;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class PoolTest extends TestBase {
	@BeforeClass
	public static void beforClass() throws IOException {
		System.out.println("Queuelet container start");
		setupContainer("PoolTest");
	}
	@After
	public void afterClass() {
		stopContainer();
		System.out.println("Queuelet container stop");
	}
	
	@Test
	public void testBuffer() throws Throwable{
		callTest("qtestBuffer");
	}
	public void qtestBuffer(){
		Pool p=PoolManager.getBufferPool(1234);
		assertEquals(p.getSequence(),0);
		assertEquals(p.getPoolBackCount(),0);
		assertEquals(p.getInstanceCount(),1);
		ByteBuffer buffer=PoolManager.getBufferInstance();
		assertEquals(p.getSequence(),1);
		assertEquals(p.getInstanceCount(),1);
		assertEquals(buffer.position(), 0);
		assertEquals(buffer.limit(), 1234);
		PoolManager.poolBufferInstance(buffer);
		assertEquals(p.getPoolBackCount(),1);
		assertEquals(p.getInstanceCount(),1);
	}

	//再利用されている事
	@Test
	public void testBufferReuse() throws Throwable{
		callTest("qtestBufferReuse");
	}
	public void qtestBufferReuse(){
		ByteBuffer b=PoolManager.getBufferInstance();
		for(int i=0;i<16;i++){
			b.put("aaaaaaaaaaaa".getBytes());
			PoolManager.poolBufferInstance(b);
			ByteBuffer buffer=PoolManager.getBufferInstance();
			assertEquals(buffer.position(), 0);
			assertEquals(buffer.limit(), 1234);
			assertSame(b,buffer);
			b=buffer;
		}
		PoolManager.poolBufferInstance(b);
	}
	
	//オーバフローする事
	//@Test
	//limitは、pool数の最大でありpoolに残りがなければnewして作られるだけ
	//以前の実装は、newする回数をlimitで指定していたが意味を変更した
	public void testBufferOverFlow() throws Throwable{
		callTest("qtestBufferOverFlow");
	}
	public void qtestBufferOverFlow(){
		ByteBuffer[] buffers=new ByteBuffer[16];
		for(int i=0;i<buffers.length;i++){
			buffers[i]=PoolManager.getBufferInstance();
		}
		ByteBuffer buffer1=PoolManager.getBufferInstance();
		assertSame(null,buffer1);
		for(int i=0;i<buffers.length;i++){
			PoolManager.poolBufferInstance(buffers[i]);
		}
	}
	
	//GC経由で再利用される事
	@Test
	public void testBufferGC() throws Throwable{
		callTest("qtestBufferGC");
	}
	public void qtestBufferGC() throws InterruptedException{
		Pool p=PoolManager.getBufferPool(1234);
		
		byte[] array;
		ByteBuffer buffer=PoolManager.getBufferInstance();
		array=buffer.array();
		buffer=null;
		System.gc();
		Thread.sleep(200);
		ByteBuffer buffer2=PoolManager.getBufferInstance();
		assertSame(array,buffer2.array());
		PoolManager.poolBufferInstance(buffer2);
		
		assertEquals(1,p.getGcCount());
	}
	
	
	//duplicate経由で再利用される事
	@Test
	public void testBufferDuplicate() throws Throwable{
		callTest("qtestBufferDuplicate");
	}
	public void qtestBufferDuplicate() throws InterruptedException{
		byte[] array;
		ByteBuffer buffer=PoolManager.getBufferInstance();
		ByteBuffer buffer2=PoolManager.duplicateBuffer(buffer);
		assertSame(buffer.array(),buffer2.array());
		array=buffer.array();
		PoolManager.poolBufferInstance(buffer);
		System.gc();
		Thread.sleep(200);
		ByteBuffer buffer3=PoolManager.getBufferInstance();
		assertNotSame(array,buffer3.array());
		PoolManager.poolBufferInstance(buffer2);
		System.gc();
		Thread.sleep(200);
		ByteBuffer buffer4=PoolManager.getBufferInstance();
		assertSame(array,buffer4.array());
		PoolManager.poolBufferInstance(buffer3);
		PoolManager.poolBufferInstance(buffer4);
	}
	
}
