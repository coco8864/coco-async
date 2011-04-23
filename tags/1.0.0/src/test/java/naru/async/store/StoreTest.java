package naru.async.store;

import static org.junit.Assert.*;

import java.io.IOException;
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


public class StoreTest extends TestBase implements BufferGetter{
	@BeforeClass
	public static void beforClass() throws IOException {
		System.out.println("Queuelet container start");
		setupContainer("StoreTest");
	}
	@AfterClass
	public static void afterClass() {
		stopContainer();
		System.out.println("Queuelet container stop");
	}
	
	private boolean isReadWrite=false;
	
	@Test
	public void testPutStore() throws Throwable{
		callTest("qtestPutStore");
	}
	public void qtestPutStore() throws Throwable{
		start();
		Store store=Store.open(true);
		BuffersTester bt=new BuffersTester();
		long start=System.currentTimeMillis();
		long total=0;
		while(true){
			ByteBuffer buffer=bt.getBuffer();
			store.putBuffer(new ByteBuffer[]{buffer});
			total+=buffer.limit();
			if((System.currentTimeMillis()-start)>=1000){
				break;
			}
		}
		System.out.println("totalPut:"+total);
		store.close();
		PoolManager.dump();
		if(error!=null){
			throw error;
		}
	}
	
	@Test
	public void testReadStore() throws Throwable{
		callTest("qtestReadStore");
	}
	public void qtestReadStore() throws Throwable{
		start();
		Store store=Store.open(true);
		BuffersTester bt=new BuffersTester();
		long start=System.currentTimeMillis();
		long total=0;
		while(true){
			ByteBuffer buffer=bt.getBuffer();
			store.putBuffer(new ByteBuffer[]{buffer});
			total+=buffer.limit();
			if((System.currentTimeMillis()-start)>=1000){
				break;
			}
		}
		System.out.println("totalPut:"+total);
		String digetst=null;
		synchronized(this){
			store.close(this, store);
			digetst=store.getDigest();
			wait();
		}
		PoolManager.dump();
		if(error!=null){
			throw error;
		}
		
		start();
		start=System.currentTimeMillis();
		store=Store.open(digetst);
		store.asyncBuffer(this,store);
		System.out.println("getGetLength:"+store.getGetLength());
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
	
	private void start(){
		putTotal=-1;
		onBufferSize=0;
		error=null;
		check=new BuffersTester();
		
	}
	private long putTotal=-1;
	private long onBufferSize=0;
	private Throwable error=null;
	private BuffersTester check=new BuffersTester();
	
	@Test
	public void testRWStore() throws Throwable{
		callTest("qtestRWStore");
	}
	public void qtestRWStore() throws Throwable{
		start();
		Store store=Store.open(false);
		isReadWrite=true;
		BuffersTester bt=new BuffersTester();
		long start=System.currentTimeMillis();
		long total=0;
		while(true){
			ByteBuffer buffer=bt.getBuffer();
			store.putBuffer(new ByteBuffer[]{buffer});
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
	
	public boolean onBuffer(Object userContext, ByteBuffer[] buffers) {
		try{
			onBufferSize+=BuffersUtil.remaining(buffers);
			check.putBuffer(buffers);
			if(putTotal==onBufferSize){
				Store store=(Store)userContext;
				if(isReadWrite){
					store.close(this, store);
				}else{
					store.close();
				}
			}
		}catch(Throwable t){
			System.out.println("error onBufferSize:"+onBufferSize);
			Store store=(Store)userContext;
			store.close();
			error=t;
		}
		return true;
	}
	public void onBufferEnd(Object userContext) {
		System.out.println("onBufferSize:"+onBufferSize);
		synchronized(this){
			notify();
		}
	}

	public void onBufferFailure(Object userContext, Throwable falure) {
		Store store=(Store)userContext;
		store.close();
		error=falure;
	}
}
