package naru.async.pool;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;


import naru.async.core.ChannelContext;
import naru.async.pool.Pool;
import naru.async.pool.PoolManager;
import naru.queuelet.test.TestBase;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ClassPoolTest extends TestBase {
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
	public void testClass() throws Throwable{
		callTest("qtestClass");
	}
	public void qtestClass(){
		Object o=PoolManager.getInstance(ChannelContext.class);
		assertEquals(o.getClass(),ChannelContext.class);
		ChannelContext a=(ChannelContext)o;
		assertEquals(a.getPoolId(),1);
		Pool p=PoolManager.getClassPool(ChannelContext.class);
		
		assertEquals(p.getSequence(),1);
		assertEquals(p.getPoolBackCount(),0);
		assertEquals(p.getInstanceCount(),16);
		Object o2=PoolManager.getInstance(ChannelContext.class);
		assertEquals(p.getSequence(),2);
		assertEquals(p.getInstanceCount(),16);
		PoolManager.poolInstance(o);
		assertEquals(1,p.getPoolBackCount());
		assertEquals(16,p.getInstanceCount());
		
		Object o3=PoolManager.getInstance(ChannelContext.class);
		assertSame(o,o3);
		PoolManager.poolInstance(o2);
		PoolManager.poolInstance(o3);
	}
	
	@Test
	public void testClass2() throws Throwable{
		callTest("qtestClass2");
	}
	public void qtestClass2(){
		Pool p=PoolManager.getClassPool(ChannelContext.class);
		Object o=PoolManager.getInstance(ChannelContext.class);
		assertEquals(o.getClass(),ChannelContext.class);
		long poolCount=p.getPoolBackCount();
		PoolManager.poolInstance(o);
		assertEquals(poolCount+1,p.getPoolBackCount());
		PoolManager.poolInstance(o);
		assertEquals(poolCount+1,p.getPoolBackCount());
	}
	
	//GCテスト
	@Test
	public void testClass5() throws Throwable{
		callTest("qtestClass5");
	}
	public void qtestClass5() throws InterruptedException{
		Object o=PoolManager.getInstance(HashMap.class);
		assertEquals(o.getClass(),HashMap.class);
		Pool p=PoolManager.getClassPool(HashMap.class);
		
		o=null;
		assertEquals(15,p.getPoolCount());
		
		System.gc();
		Thread.sleep(200);
		
		assertEquals(16,p.getLimit());
		assertEquals(1,p.getGcCount());//一般クラスはGC通知される
		p.info(true);
	}
	
	public static class PB extends PoolBase{
	}
	
	//GCテスト
	@Test
	public void testClass6() throws Throwable{
		callTest("qtestClass6");
	}
	public void qtestClass6() throws InterruptedException{
		Object o=PoolManager.getInstance(PB.class);
		assertEquals(o.getClass(),PB.class);
		Pool p=PoolManager.getClassPool(PB.class);
		
		o=null;
		assertEquals(0,p.getPoolCount());
		
		System.gc();
		Thread.sleep(200);
		
		assertEquals(16,p.getLimit());
		assertEquals(1,p.getGcCount());//PoolBaseを継承したクラスはGC通知される
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
			PB pb=(PB)PoolManager.getInstance(PB.class);
			pb.unref(true);
			count++;
			if((System.currentTimeMillis()-start)>limitTime){
				break;
			}
		}
		System.out.println("PoolBase pool:"+count +" "+(float)count/(float)limitTime+"/ms");
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
			HashMap map=(HashMap)PoolManager.getInstance(HashMap.class);
			PoolManager.poolInstance(map);
			count++;
			if((System.currentTimeMillis()-start)>limitTime){
				break;
			}
		}
		System.out.println("PoolBase pool:"+count +" "+(float)count/(float)limitTime+"/ms");
	}
	
}
