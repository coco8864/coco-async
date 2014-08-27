package naru.async.store;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolManager;
import naru.async.store.Page;
import naru.queuelet.test.TestBase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class PageTest extends TestBase {
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		System.out.println("!!!setUpBeforeClass!!!");
		TestBase.setupContainer(
				"testEnv.properties",
				"Store2Test");
	}
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		TestBase.stopContainer();
		System.out.println("!!!tearDownAfterClass!!!");
	}
	
	@Test
	public void testPage1() throws Throwable{
		System.out.println("!!!testPage1!!!");
		callTest("qtestPage1");
	}
	public void qtestPage1() throws Throwable{
		Page page=(Page)PoolManager.getInstance(Page.class);
		int i=0;
		while(true){
			ByteBuffer buffer=PoolManager.getBufferInstance();
			buffer.put("0123456789abcdef".getBytes());
			buffer.flip();
			if( !page.putBuffer(buffer,false) ){
				break;
			}
			i++;
		}
		assertEquals(i*16,page.getBufferLength());
	}
	
	@Test
	public void testPage2() throws Throwable{
		callTest("qtestPage2");
	}
	public void qtestPage2() throws Throwable{
		Page page=(Page)PoolManager.getInstance(Page.class);
		page.putBytes("1111".getBytes());
		page.putBytes("2222".getBytes());
		page.putBytes("3333".getBytes());
		page.putBytes("4444".getBytes());
		ByteBuffer[] buf=page.getBuffer();
		assertEquals("1111222233334444",BuffersUtil.toStringFromBuffer(buf[0],"utf-8"));
	}
	

}
