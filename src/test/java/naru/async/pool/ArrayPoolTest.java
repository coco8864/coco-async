package naru.async.pool;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;

import naru.async.pool.Pool;
import naru.async.pool.PoolManager;
import naru.async.pool.ClassPoolTest.PB;
import naru.queuelet.test.TestBase;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ArrayPoolTest extends TestBase {
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
	public void testArray() throws Throwable{
		callTest("qtestArray");
	}
	public void qtestArray(){
		Object o=PoolManager.getArrayInstance(byte.class,1234);
		assertEquals(o.getClass(),byte[].class);
		byte[] a=(byte[])o;
		assertEquals(1234,a.length);
		
		Pool p=PoolManager.getArrayPool(byte.class,1234);
		assertEquals(1,p.getSequence());
		assertEquals(0,p.getPoolBackCount());
		assertEquals(1,p.getInstanceCount());
		assertEquals(16,p.getLimit());
		assertEquals(0,p.getGcCount());
		
		Object o2=PoolManager.getArrayInstance(byte.class,1234);
		assertEquals(2,p.getSequence());
		assertEquals(2,p.getInstanceCount());
		
		PoolManager.poolArrayInstance(o);
		assertEquals(1,p.getPoolBackCount());
		assertEquals(2,p.getInstanceCount());
		
		Object o3=PoolManager.getArrayInstance(byte.class,1234);
		assertSame(o,o3);
		PoolManager.poolArrayInstance(o2);
		PoolManager.poolArrayInstance(o3);
		
		assertEquals(3,p.getPoolBackCount());
		assertEquals(2,p.getInstanceCount());
		p.info(true);
	}
	
	@Test
	public void testArray2() throws Throwable{
		callTest("qtestArray2");
	}
	public void qtestArray2(){
		Object o=PoolManager.getArrayInstance(String.class,1234);
		assertEquals(o.getClass(),String[].class);
		String[] a=(String[])o;
		assertEquals(1234,a.length);
		Pool p=PoolManager.getArrayPool(String.class,1234);
		assertEquals(1,p.getSequence());
		assertEquals(0,p.getPoolBackCount());
		assertEquals(1,p.getInstanceCount());
		assertEquals(16,p.getLimit());
		assertEquals(0,p.getGcCount());
		
		Object o2=PoolManager.getArrayInstance(String.class,1234);
		assertEquals(2,p.getSequence());
		assertEquals(2,p.getInstanceCount());
		PoolManager.poolArrayInstance(o);
		assertEquals(1,p.getPoolBackCount());
		assertEquals(2,p.getInstanceCount());
		
		Object o3=PoolManager.getArrayInstance(String.class,1234);
		assertSame(o,o3);
		PoolManager.poolArrayInstance(o2);
		PoolManager.poolArrayInstance(o3);
		
		assertEquals(3,p.getPoolBackCount());
		assertEquals(2,p.getInstanceCount());
		p.info(true);
	}
	
	//２重開放テスト
	@Test
	public void testArray3() throws Throwable{
		callTest("qtestArray3");
	}
	public void qtestArray3(){
		Object o=PoolManager.getArrayInstance(String.class,1234);
		assertEquals(o.getClass(),String[].class);
		String[] a=(String[])o;
		assertEquals(1234,a.length);
		Pool p=PoolManager.getArrayPool(String.class,1234);
		
		PoolManager.poolArrayInstance(o);
		PoolManager.poolArrayInstance(o);
		
		assertEquals(1,p.getSequence());
		assertEquals(1,p.getPoolBackCount());
		assertEquals(1,p.getInstanceCount());
		assertEquals(16,p.getLimit());
		assertEquals(0,p.getGcCount());
		p.info(true);
	}
	
	//overflowテスト
	@Test
	public void testArray4() throws Throwable{
		callTest("qtestArray4");
	}
	public void qtestArray4() throws InterruptedException{
		Object o=PoolManager.getArrayInstance(byte.class,1234);
		assertEquals(o.getClass(),byte[].class);
		byte[] a=(byte[])o;
		assertEquals(1234,a.length);
		Pool p=PoolManager.getArrayPool(byte.class,1234);
		
		int limit=p.getLimit();
		Object ao[]=new Object[limit+10];
		for(int i=0;i<limit+10;i++){
			ao[i]=PoolManager.getArrayInstance(byte.class,1234);
		}
		assertEquals(limit+11,p.getSequence());
		assertEquals(0,p.getPoolBackCount());
		assertEquals(limit+11,p.getInstanceCount());
		assertEquals(16,p.getLimit());
		assertEquals(0,p.getGcCount());
		
		for(int i=0;i<limit+10;i++){
			PoolManager.poolArrayInstance(ao[i]);
		}
		PoolManager.poolArrayInstance(o);
		assertEquals(limit,p.getPoolCount());
		
		System.gc();
		Thread.sleep(200);
		
		assertEquals(limit+11,p.getSequence());
		//pool数は、limitから増えない
		assertEquals(limit,p.getPoolCount());
		assertEquals(limit+11,p.getInstanceCount());
		assertEquals(128,p.getLimit());
		assertEquals(0,p.getGcCount());
		p.info(true);
	}
	
	//GCテスト
	@Test
	public void testArray5() throws Throwable{
		callTest("qtestArray5");
	}
	public void qtestArray5() throws InterruptedException{
		Object o=PoolManager.getArrayInstance(byte.class,1234);
		assertEquals(o.getClass(),byte[].class);
		byte[] a=(byte[])o;
		assertEquals(1234,a.length);
		Pool p=PoolManager.getArrayPool(byte.class,1234);
		
		o=null;
		assertEquals(0,p.getPoolCount());
		
		System.gc();
		Thread.sleep(200);
		
		assertEquals(16,p.getLimit());
		assertEquals(0,p.getGcCount());//arrayは、GC通知されない
		p.info(true);
	}
	
	//性能テスト
	@Test
	public void testPerfom1() throws Throwable{
		callTest("qtestPerfom1");
	}
	public void qtestPerfom1() throws InterruptedException{
		long count=0;
		long limitTime=5000;
		long start=System.currentTimeMillis();
		while(true){
			Object o=PoolManager.getArrayInstance(byte.class,1234);
			PoolManager.poolArrayInstance(o);
			count++;
			if((System.currentTimeMillis()-start)>limitTime){
				break;
			}
		}
		System.out.println("Array pool:"+count +" "+(float)count/(float)limitTime+"/ms");
	}
	@Test
	public void testPerfom2() throws Throwable{
		callTest("qtestPerfom2");
	}
	public void qtestPerfom2() throws InterruptedException{
		long count=0;
		long limitTime=5000;
		long start=System.currentTimeMillis();
		while(true){
			ByteBuffer buffer=PoolManager.getBufferInstance();
			PoolManager.poolBufferInstance(buffer);
			count++;
			if((System.currentTimeMillis()-start)>limitTime){
				break;
			}
		}
		System.out.println("ByteBuffer pool:"+count +" "+(float)count/(float)limitTime+"/ms");
	}
}
