package naru.async.cache;

import static org.junit.Assert.*;

import java.io.File;
import java.nio.ByteBuffer;

import naru.async.BufferGetter;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolManager;
import naru.async.store.Page;
import naru.queuelet.test.TestBase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class FileTest extends TestBase {
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		System.out.println("!!!setUpBeforeClass!!!");
		TestBase.setupContainer(
				"queueletTest.properties",
				"CoreTest");
	}
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		TestBase.stopContainer();
		System.out.println("!!!tearDownAfterClass!!!");
	}
	
	private static class Getter implements BufferGetter{
		public boolean onBuffer(Object userContext, ByteBuffer[] buffers) {
			ByteBuffer b=buffers[0];
			System.out.println("onBuffer.length:"+BuffersUtil.remaining(buffers));
			System.out.println("hashCode:"+System.identityHashCode(b)+":array:"+b.array());
			PoolManager.poolBufferInstance(buffers);
			return true;
		}
		public void onBufferEnd(Object userContext) {
			System.out.println("onBufferEnd");
			((AsyncFile)userContext).close();
		}
		public void onBufferFailure(Object userContext, Throwable failure) {
			System.out.println("onBufferFailure");
			((AsyncFile)userContext).close();
		}
	}
	
	@Test
	public void test1() throws Throwable{
		System.out.println("!!!testPage1!!!");
		callTest("qtest1",Long.MAX_VALUE);
	}
	
	public void qtest1() throws Throwable{
		AsyncFile asyncFile=AsyncFile.open(new File("pom.xml"));
		asyncFile.asyncRead(new Getter(), asyncFile);
	}
	
	@Test
	public void test2() throws Throwable{
		System.out.println("!!!testPage1!!!");
		callTest("qtest2",Long.MAX_VALUE);
	}
	
	public void qtest2() throws Throwable{
		AsyncFile asyncFile=AsyncFile.open(new File("pom.xml"));
		AsyncFile asyncFile2=AsyncFile.open(new File("pom.xml"));
		AsyncFile asyncFile3=AsyncFile.open(new File("pom.xml"));
		asyncFile.asyncRead(new Getter(), asyncFile);
		asyncFile2.asyncRead(new Getter(), asyncFile2);
		asyncFile3.asyncRead(new Getter(), asyncFile3);
		Thread.sleep(1000);
	}
	
}
