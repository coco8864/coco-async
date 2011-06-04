package naru.async.store;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import naru.async.BufferGenerator;
import naru.async.store.Store;
import naru.async.store.StoreManager;
import naru.queuelet.test.TestBase;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class StorePutTest extends TestBase{
	@BeforeClass
	public static void beforClass() throws IOException {
		setupContainer();
	}
	@Before
	public void beforeTest() {
		//storeを初期化するコード
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
		BufferGenerator generator=new BufferGenerator(length,0);
		
		long start=System.currentTimeMillis();
		Store store=Store.open(true);
		long storeId=store.getStoreId();
		assertTrue(Store.isLiveStoreId(storeId));
		
		Iterator<ByteBuffer[]>itr=generator.bufferIterator();
		while(itr.hasNext()){
			store.putBuffer(itr.next());
		}
		System.out.println("putend time:"+(System.currentTimeMillis()-start));
		store.close(true,true);
		System.out.println("end time:"+(System.currentTimeMillis()-start));
		String storeDigest=store.getDigest();
		long storeLength=StoreManager.getStoreLength(storeDigest);
		assertTrue(Store.isLiveStoreId(storeId));//保存したデータは生きている
		assertTrue("格納したデータが正しいか？",generator.check(storeLength, storeDigest));
		assertEquals("storeId",1,StoreManager.getStoreId(storeDigest));
		assertEquals("ref",1,StoreManager.getStoreRefCount(storeDigest));

		start=System.currentTimeMillis();
		store=Store.open(true);
		itr=generator.bufferIterator();
		while(itr.hasNext()){
			store.putBuffer(itr.next());//１つづつputBuffer
		}
		System.out.println("putend time:"+(System.currentTimeMillis()-start));
		store.close(true,true);
		System.out.println("end time:"+(System.currentTimeMillis()-start));
		assertEquals("ref",2,StoreManager.getStoreRefCount(storeDigest));
		
		generator.term();
		System.out.println(StoreManager.infoStoreStastics());
	}
	
	@Test
	public void test02() throws Throwable{
		callTest("qtest02");
	}
	
	public void qtest02() throws Throwable{
		long length=10240000;
		BufferGenerator generator=new BufferGenerator(length,0);
		
		long start=System.currentTimeMillis();
		Store store=Store.open(true);
		ByteBuffer[] buffers=generator.getBuffer();
		store.putBuffer(buffers);//一気にputBuffer
		System.out.println("putend time:"+(System.currentTimeMillis()-start));
		store.close(true,true);
		System.out.println("end time:"+(System.currentTimeMillis()-start));
		String storeDigest=store.getDigest();
		long storeLength=StoreManager.getStoreLength(storeDigest);
		assertTrue("格納したデータが正しいか？",generator.check(storeLength, storeDigest));
		assertEquals("storeId",1,StoreManager.getStoreId(storeDigest));
		assertEquals("ref",1,StoreManager.getStoreRefCount(storeDigest));
		generator.term();
	}
	
	@Test
	public void test03() throws Throwable{
		callTest("qtest03");
	}
	
	public void qtest03() throws Throwable{
		long length=10240000;
		BufferGenerator generator=new BufferGenerator(length,0);
		
		long start=System.currentTimeMillis();
		Store store=Store.open(true);
		Iterator<ByteBuffer[]>itr=generator.bufferIterator();
		while(itr.hasNext()){
			store.putBuffer(itr.next());
		}
		System.out.println("putend time:"+(System.currentTimeMillis()-start));
		store.close(true,true);
		System.out.println("end time:"+(System.currentTimeMillis()-start));
		String storeDigest=store.getDigest();
		long storeLength=StoreManager.getStoreLength(storeDigest);
		assertTrue("格納したデータが正しいか？",generator.check(storeLength, storeDigest));
		assertEquals("storeId",1,StoreManager.getStoreId(storeDigest));
		assertEquals("ref",1,StoreManager.getStoreRefCount(storeDigest));
	}
	
	
	/*
	@Test
	public void testSaveToFile() throws Throwable{
		callTest("qtestSaveToFile");
	}
	public void qtestSaveToFile() throws Throwable{
		Set ids=StoreManager.listPersistenceStoreId();
		Iterator<Long> itr=ids.iterator();
		while(itr.hasNext()){
			long storeId=itr.next();
			String fileName="test"+ storeId + ".dat";
			OutputStream os=new FileOutputStream(fileName);
			StoreStream.storeToStream(storeId, os);
			os.close();

			InputStream is=new FileInputStream(fileName);
			String digest=StoreStream.streamToStore(is);
			is.close();
			GetTester tester=new GetTester(digest,1);
			tester.waitForEnd();
			File file=new File(fileName);
			System.out.println("storeId:"+storeId+"->"+digest +":size:"+file.length()+":"+file);
			file.delete();
			StoreManager.unref(digest);
		}
	}
	*/
	/*
	@Test
	public void testDelete() throws Throwable{
		callTest("qtestDelete");
	}
	public void qtestDelete() throws Throwable{
		int count=StoreManager.getPersistenceStoresCount();
		System.out.println("count:"+count);
		for(long storeId:storeIds){
			long length=StoreManager.getStoreLength(storeId);
			String digest=StoreManager.getStoreDigest(storeId);
			System.out.println("storeId:"+storeId + " length:"+length+":digest:"+digest);
			if(length>=0){
				StoreManager.removeStore(storeId);
				count--;
			}
		}
		assertEquals(count,StoreManager.getPersistenceStoresCount());
	}
	*/
	
}
