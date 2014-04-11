package naru.async.store;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.Iterator;

import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolManager;
import naru.async.store.Store;
import naru.async.store.StoreManager;
import naru.queuelet.test.TestBase;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class StoreCompressTest extends TestBase{
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestBase.setupContainer(
				"testEnv.properties",
				"StoreCompressTest");
	}
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		TestBase.stopContainer();
	}
	
	@Test
	public void test1() throws Throwable{
		callTest("qtest1");
	}
	public void qtest1() throws Throwable{
		for(int i=1;i<=4;i++){
			PutGetTester pgt=new PutGetTester(102400,100,i);
			pgt.waitForEnd();
			pgt.check();
			System.out.println("end of "+i);
		}
	}
	
}
