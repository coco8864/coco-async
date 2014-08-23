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
			((Cache)userContext).close();
		}
		public void onBufferFailure(Object userContext, Throwable failure) {
			System.out.println("onBufferFailure");
			((Cache)userContext).close();
		}
	}
	
	@Test
	public void test1() throws Throwable{
		System.out.println("!!!testPage1!!!");
		callTest("qtest1",Long.MAX_VALUE);
	}
	
	public void qtest1() throws Throwable{
		Cache asyncFile=Cache.open(new File("pom.xml"));
		asyncFile.asyncBuffer(new Getter(), asyncFile);
	}
	
	@Test
	public void test2() throws Throwable{
		System.out.println("!!!testPage1!!!");
		callTest("qtest2",Long.MAX_VALUE);
	}
	
	public void qtest2() throws Throwable{
		Cache asyncFile=Cache.open(new File("pom.xml"));
		Cache asyncFile2=Cache.open(new File("pom.xml"));
		Cache asyncFile3=Cache.open(new File("pom.xml"));
		asyncFile.asyncBuffer(new Getter(), asyncFile);
		asyncFile2.asyncBuffer(new Getter(), asyncFile2);
		asyncFile3.asyncBuffer(new Getter(), asyncFile3);
		Thread.sleep(1000);
	}
	
	@Test
	public void test3() throws Throwable{
		System.out.println("!!!testPage3!!!");
		callTest("qtest3",Long.MAX_VALUE);
	}
	public void qtest3() throws Throwable{
		Cache asyncFile=Cache.open();
		asyncFile.putBuffer(ByteBuffer.wrap("abcdefg".getBytes()));
		asyncFile.putBuffer(ByteBuffer.wrap("ABCDEFG".getBytes()));
		asyncFile.flip();
		ByteBuffer b[]=asyncFile.popTopBuffer();
		System.out.println(BuffersUtil.toStringFromBuffer(b[0], "utf-8"));
		asyncFile.asyncBuffer(new Getter(), asyncFile);
		Thread.sleep(1000);
	}
	
	
}
