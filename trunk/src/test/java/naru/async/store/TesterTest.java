package naru.async.store;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;

import naru.async.BufferGetter;
import naru.async.BuffersTester;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolManager;
import naru.async.store.Store;
import naru.async.timer.TimerManager;
import naru.queuelet.test.TestBase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TesterTest extends TestBase{
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
	
	@Test
	public void test1() throws Throwable{
		callTest("qtest1",Long.MAX_VALUE);
	}
	public void qtest1() throws Throwable{
		BuffersTester getter=new BuffersTester();
		BuffersTester putter=new BuffersTester();
		long start=System.currentTimeMillis();
		int loop=0;
		long length=0;
		for(;;loop++){
			ByteBuffer buffers[]=getter.getBuffers();
			length+=BuffersUtil.remaining(buffers);
			putter.putBuffer(buffers);
			if((System.currentTimeMillis()-start)>10000){
				break;
			}
		}
		putter.check();
		System.out.println("loop:"+loop +":length:"+length+":rate:"+(double)length/(double)(System.currentTimeMillis()-start)+"byte/ms");
	}
}
