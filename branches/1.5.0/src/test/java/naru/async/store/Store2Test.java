package naru.async.store;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import naru.async.BufferGetter;
import naru.async.BuffersTester;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolManager;
import naru.async.store.Store;
import naru.queuelet.test.TestBase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class Store2Test extends TestBase implements BufferGetter{
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestBase.setupContainer(
				"testEnv.properties",
				"Store2Test");
	}
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		TestBase.stopContainer();
	}
	
	private long sid;
	
	@Test
	public void testPutStore() throws Throwable{
		callTest("qtestPutStore");
	}
	public void qtestPutStore() throws Throwable{
		start();
		Store store=Store.open(true);
		BuffersTester bt=new BuffersTester();
		long start=System.currentTimeMillis();
		System.out.println("start:"+start);
		long total=0;
		while(true){
			ByteBuffer buffer=bt.getBuffer();
			store.putBuffer(BuffersUtil.toByteBufferArray(buffer));
			total+=buffer.limit();
			if((System.currentTimeMillis()-start)>=1000){
				break;
			}
		}
		System.out.println("wait out time1:"+(System.currentTimeMillis()-start));
		sid=store.getStoreId();
		System.out.println("sid:"+sid);
		System.out.println("totalPut:"+total);
		store.close(this,"owari");
		synchronized(this){
			try{
				wait();
			} catch (InterruptedException e) {
			}
		}
		System.out.println("wait out time2:"+(System.currentTimeMillis()-start));
		PoolManager.dump();
		if(error!=null){
			throw error;
		}
	}
	
	@Test
	public void testGetStore() throws Throwable{
		callTest("qtestGetStore");
	}
	public void qtestGetStore() throws Throwable{
		start();
		long start=System.currentTimeMillis();
		Store store=Store.open(sid);
		store.asyncBuffer(this,store);
		System.out.println("sid:"+store.getStoreId());
		System.out.println("getPutLength:"+store.getPutLength());
		synchronized(this){
			try{
				wait();
			} catch (InterruptedException e) {
			}
		}
//		store.close();
		System.out.println("wait out time:"+(System.currentTimeMillis()-start));
		System.gc();
		PoolManager.dump();
		if(error!=null){
			throw error;
		}
	}
	
	private long putTotal=-1;
	private long onBufferSize=0;
	private Throwable error=null;
	private BuffersTester tester=new BuffersTester();
	private void start(){
		putTotal=-1;
		onBufferSize=0;
		error=null;
		tester=new BuffersTester();
		
	}
	private void check() throws Throwable{
		if(error!=null){
			throw error;
		}
		tester.check();
	}
	
	
	@Test
	public void testRWStore() throws Throwable{
		callTest("qtestRWStore");
	}
	public void qtestRWStore() throws Throwable{
		start();
		Store store=Store.open(false);
		BuffersTester bt=new BuffersTester();
		long start=System.currentTimeMillis();
		long total=0;
		while(true){
			ByteBuffer buffer=bt.getBuffer();
			store.putBuffer(BuffersUtil.toByteBufferArray(buffer));
			total+=buffer.limit();
			if((System.currentTimeMillis()-start)>=1000){
				break;
			}
		}
		putTotal=total;
		store.asyncBuffer(this,store);
		System.out.println("totalPut:"+total);
		synchronized(this){
			try{
				wait();
			} catch (InterruptedException e) {
			}
		}
		System.out.println("wait out time:"+(System.currentTimeMillis()-start));
		System.gc();
		PoolManager.dump();
		if(error!=null){
			throw error;
		}
	}
	
	public boolean onBuffer(ByteBuffer[] buffers, Object userContext) {
		onBufferSize+=BuffersUtil.remaining(buffers);
		System.out.println("onBuffer:onBufferSize:"+onBufferSize);
		tester.putBuffer(buffers);
		if(putTotal==onBufferSize){
			Store store=(Store)userContext;
			store.close(this,store);
		}
		return true;
	}
	public void onBufferEnd(Object userContext) {
		System.out.println("onBufferSize:"+onBufferSize);
		synchronized(this){
			notify();
		}
	}
	public void onBufferFailure(Throwable falure, Object userContext) {
		Store store=(Store)userContext;
		store.close();
		error=falure;
	}
}
