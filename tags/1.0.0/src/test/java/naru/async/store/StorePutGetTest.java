package naru.async.store;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import naru.async.BufferGenerator;
import naru.async.BufferGetter;
import naru.async.BuffersTester;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolManager;
import naru.async.store.Store;
import naru.async.timer.TimerManager;
import naru.queuelet.test.TestBase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class StorePutGetTest extends TestBase implements BufferGetter{
	@BeforeClass
	public static void beforClass() throws IOException {
		setupContainer();
	}
	@Before
	public void beforeTest() {
		//storeを初期化するコード
		/*
		String storeDir=getProperty("storeDir");
		if(storeDir==null){
			throw new IllegalStateException("fail to get storeDir");
		}
		File dir=new File(storeDir);
		if(dir.exists()){
			File persistenceStore=new File(dir,"persistenceStore.sar");
			if(persistenceStore.exists()){
				persistenceStore.delete();
			}
		}else{
			dir.mkdir();
		}
		System.out.println("storeDir:"+storeDir);
		*/
		
		System.out.println("Queuelet container start");
		startContainer("StoreTest");
	}
	
	@After
	public void afterTest() {
		stopContainer();
		System.out.println("Queuelet container stop");
	}
	
	@Test
	public void test01() throws Throwable{
		callTest("qtest01");
	}
	
	public void qtest01() throws Throwable{
		long length=10240000;
		BufferGenerator generator=new BufferGenerator(length,1);
		
		long start=System.currentTimeMillis();
		assertEquals(0,Store.getPlainStoresCount());
		Store store=Store.open(false);
		assertEquals(1,Store.getPlainStoresCount());
		long storeId=store.getStoreId();
		assertTrue(Store.isLiveStoreId(storeId));
		
		Iterator<ByteBuffer[]>itr=generator.bufferIterator();
		while(itr.hasNext()){
			store.putBuffer(itr.next());
		}
		System.out.println("putend time:"+(System.currentTimeMillis()-start));
		BufferGenerator checker=new BufferGenerator();
		start=System.currentTimeMillis();
		String digest=generator.getDigest();
		generator.term();
		synchronized(checker){
			store.close(this, checker);
			checker.wait();
		}
		assertEquals(length, store.getGetLength());
		assertEquals(length, store.getPutLength());
		assertFalse("closeしたら有効じゃない",Store.isLiveStoreId(storeId));
		assertTrue("受信データが正しいか?",checker.check(length, digest));
		System.out.println("check:"+checker.check(length, digest)+":time:"+(System.currentTimeMillis()-start));
		System.out.println(StoreManager.infoStoreStastics());
	}
	
	@Test
	public void test02() throws Throwable{
		callTest("qtest02",Long.MAX_VALUE);
	}
	
	public void qtest02() throws Throwable{
		long length=10240000;
		BufferGenerator generator=new BufferGenerator(length,1);
		
		long start=System.currentTimeMillis();
		Store store=Store.open(false);
		BufferGenerator checker=new BufferGenerator();
		store.asyncBuffer(this, checker);
		Iterator<ByteBuffer[]>itr=generator.bufferIterator();
		while(itr.hasNext()){
			store.putBuffer(itr.next());
		}
		System.out.println("putend time:"+(System.currentTimeMillis()-start));
		start=System.currentTimeMillis();
		String digest=generator.getDigest();
		generator.term();
		synchronized(checker){
			store.close(this,checker);//最後まで通知
			checker.wait();
		}
		assertTrue("受信データが正しいか?",checker.check(length, digest));
		System.out.println("check:"+checker.check(length, digest)+":time:"+(System.currentTimeMillis()-start));
		System.out.println(StoreManager.infoStoreStastics());
	}
	
	@Test
	public void test03() throws Throwable{
		callTest("qtest03",Long.MAX_VALUE);
	}
	
	public void qtest03() throws Throwable{
		long length=10240000;
		BufferGenerator generator=new BufferGenerator(length,1);
		
		long start=System.currentTimeMillis();
		Store store=Store.open(false);
		BufferGenerator checker=new BufferGenerator();
		store.asyncBuffer(this, checker);
		Iterator<ByteBuffer[]>itr=generator.bufferIterator();
		while(itr.hasNext()){
			store.putBuffer(itr.next());
		}
		System.out.println("putend time:"+(System.currentTimeMillis()-start));
		start=System.currentTimeMillis();
		String digest=generator.getDigest();
		generator.term();
		synchronized(checker){
			store.close();//途中で中断
			checker.wait();
		}
		assertFalse("受信データが正しいか?",checker.check(length, digest));
		System.out.println("check:"+checker.check(length, digest)+":time:"+(System.currentTimeMillis()-start));
		System.out.println(StoreManager.infoStoreStastics());
	}
	
	public boolean onBuffer(Object userContext, ByteBuffer[] buffers) {
		BufferGenerator checker=(BufferGenerator)userContext;
		checker.put(buffers);
		return true;
	}
	private static Logger logger=Logger.getLogger(StorePutGetTest.class);
	
	public void onBufferEnd(Object userContext) {
		System.out.println("onBufferEnd");
		logger.debug("onBufferEnd");
		BufferGenerator checker=(BufferGenerator)userContext;
		checker.end();
	}
	public void onBufferFailure(Object userContext, Throwable failure) {
		System.out.println("onBufferFailure");
		failure.printStackTrace();
		BufferGenerator checker=(BufferGenerator)userContext;
		checker.end();
	}
}
