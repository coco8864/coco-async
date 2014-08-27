package naru.async.store;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import naru.async.BufferGenerator;
import naru.async.BufferGetter;
import naru.async.store.Store;
import naru.async.store.StoreManager;
import naru.queuelet.test.TestBase;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class StoreGetTest extends TestBase implements BufferGetter{
	@BeforeClass
	public static void beforClass() throws IOException {
		setupContainer();
	}
	@Before
	public void beforeTest() {
		//store������������R�[�h
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
		BufferGenerator generator=new BufferGenerator(length,1);
		
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
		long storeId=store.getStoreId();
		long storeLength=StoreManager.getStoreLength(storeDigest);
		assertTrue("�i�[�����f�[�^�����������H",generator.check(storeLength, storeDigest));
		assertEquals("storeId",1,StoreManager.getStoreId(storeDigest));
		assertEquals("ref",1,StoreManager.getStoreRefCount(storeDigest));
		generator.term();
		
		BufferGenerator checker=new BufferGenerator();
		start=System.currentTimeMillis();
		Store store3=Store.open(storeId);
		synchronized(checker){
			store3.asyncBuffer(this, checker);
			checker.wait();
		}
		Thread.sleep(10);
		assertFalse("store3���J�����ꂽ��?",store3.checkRef());
		
		//store���Ō�܂œǂݍ��񂾏ꍇ�A�J������鎖���m�F
		assertTrue("��M�f�[�^����������?",generator.check(length, storeDigest));
		System.out.println("check:"+generator.check(length, storeDigest)+":time:"+(System.currentTimeMillis()-start));
		System.out.println(StoreManager.infoStoreStastics());
	}
	
	//�Ō�܂œǂݍ��܂��A�r����close�����ꍇ�́H
	@Test
	public void test011() throws Throwable{
		callTest("qtest011");
	}
	
	public void qtest011() throws Throwable{
		long length=10240000;
		BufferGenerator generator=new BufferGenerator(length,1);
		
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
		long storeId=store.getStoreId();
		BufferGenerator checker=new BufferGenerator();
		Store store4=Store.open(storeId);
		synchronized(checker){
			store4.asyncBuffer(this, checker);
			store4.close(this, checker);
			checker.wait();
		}
		Thread.sleep(10);
		assertFalse("store4���J�����ꂽ��?",store4.checkRef());
		
		
		//store���Ō�܂œǂݍ��񂾏ꍇ�A�J������鎖���m�F
		assertTrue("��M�f�[�^����������?",generator.check(length, storeDigest));
		System.out.println("check:"+generator.check(length, storeDigest)+":time:"+(System.currentTimeMillis()-start));
		System.out.println(StoreManager.infoStoreStastics());
	}
	
	
	
	
	
	
	@Test
	public void test02() throws Throwable{
		callTest("qtest02");
	}
	
	public void qtest02() throws Throwable{
		long length=10240000;
		BufferGenerator generator=new BufferGenerator(length,1);
		
		long start=System.currentTimeMillis();
		Store store=Store.open(true);
		ByteBuffer[] bufs=generator.getBuffer();
		store.putBuffer(bufs);
		System.out.println("putend time:"+(System.currentTimeMillis()-start));
		store.close(true,true);
		String storeDigest=store.getDigest();
		long storeLength=StoreManager.getStoreLength(storeDigest);
		assertTrue("�i�[�����f�[�^�����������H",generator.check(storeLength, storeDigest));
		assertEquals("storeId",1,StoreManager.getStoreId(storeDigest));
		assertEquals("ref",1,StoreManager.getStoreRefCount(storeDigest));
		generator.term();
		
		BufferGenerator checker=new BufferGenerator();
		start=System.currentTimeMillis();
		Store store3=Store.open(storeDigest);
		synchronized(checker){
			store3.asyncBuffer(this, checker);
			checker.wait();
		}
		assertTrue("��M�f�[�^����������?",generator.check(length, storeDigest));
		System.out.println("check:"+generator.check(length, storeDigest)+":time:"+(System.currentTimeMillis()-start));
		System.out.println(StoreManager.infoStoreStastics());
	}
	
	@Test
	public void test03() throws Throwable{
		callTest("qtest03");
	}
	
	public void qtest03() throws Throwable{
		long length=10240000;
		BufferGenerator generator=new BufferGenerator(length,1);
		
		long start=System.currentTimeMillis();
		Store store=Store.open(true);
		Iterator<ByteBuffer[]>itr=generator.bufferIterator();
		while(itr.hasNext()){
			store.putBuffer(itr.next());
		}
		System.out.println("putend time:"+(System.currentTimeMillis()-start));
		store.close(true,true);
		String digest=store.getDigest();
		System.out.println("check:"+generator.check(length, digest));
		System.out.println("digest:"+store.getDigest()+":time:"+(System.currentTimeMillis()-start));
		System.out.println("id:"+StoreManager.getStoreId(digest));
		System.out.println(digest+":length:"+StoreManager.getStoreLength(digest));
		System.out.println("ref:"+StoreManager.getStoreRefCount(digest));
		generator.term();
		
		//���������ǂݍ���
		BufferGenerator checkers[]=new BufferGenerator[4];
		for(int i=0;i<checkers.length;i++){
			checkers[i]=new BufferGenerator();
		}
		start=System.currentTimeMillis();
		Store stores[]=new Store[checkers.length];
		for(int i=0;i<checkers.length;i++){
			stores[i]=Store.open(digest);
			stores[i].asyncBuffer(this, checkers[i]);
		}
		for(int i=0;i<checkers.length;i++){
			checkers[i].waitForEnd();
			assertTrue("��M�f�[�^����������?",checkers[i].check(length, digest));
			System.out.println("check:"+checkers[i].check(length, digest)+":time:"+(System.currentTimeMillis()-start));
		}
		System.out.println("end :time:"+(System.currentTimeMillis()-start));
	}
	
	@Test
	public void test04() throws Throwable{
		callTest("qtest04");
	}
	
	public void qtest04() throws Throwable{
		long length=10240000;
		BufferGenerator generator=new BufferGenerator(length,4);
		
		long start=System.currentTimeMillis();
		Store store=Store.open(true);
		Iterator<ByteBuffer[]>itr=generator.bufferIterator();
		while(itr.hasNext()){
			store.putBuffer(itr.next());
		}
		System.out.println("putend time:"+(System.currentTimeMillis()-start));
		store.close(true,true);
		String digest=store.getDigest();
		System.out.println("check:"+generator.check(length, digest));
		System.out.println("digest:"+store.getDigest()+":time:"+(System.currentTimeMillis()-start));
		System.out.println("id:"+StoreManager.getStoreId(digest));
		System.out.println(digest+":length:"+StoreManager.getStoreLength(digest));
		System.out.println("ref:"+StoreManager.getStoreRefCount(digest));
		generator.term();
		
		BufferGenerator checkers[]=new BufferGenerator[2];
		for(int i=0;i<checkers.length;i++){
			checkers[i]=new BufferGenerator();
		}
		start=System.currentTimeMillis();
		Store stores[]=new Store[checkers.length];
		for(int i=0;i<checkers.length;i++){
			stores[i]=Store.open(digest);
			stores[i].asyncBuffer(this, checkers[i]);
		}
		for(int i=0;i<checkers.length;i++){
			stores[i].close();//�r���ŃL�����Z��
		}
		for(int i=0;i<checkers.length;i++){
			checkers[i].waitForEnd();
			assertFalse("��M�f�[�^����������?",checkers[i].check(length, digest));
			System.out.println("check:"+checkers[i].check(length, digest)+":time:"+(System.currentTimeMillis()-start));
		}
		System.out.println("end :time:"+(System.currentTimeMillis()-start));
	}
	
	public boolean onBuffer(ByteBuffer[] buffers, Object userContext) {
		BufferGenerator checker=(BufferGenerator)userContext;
		checker.put(buffers);
		return true;
	}
	public void onBufferEnd(Object userContext) {
		System.out.println("onBufferEnd");
		BufferGenerator checker=(BufferGenerator)userContext;
		checker.end();
	}
	public void onBufferFailure(Throwable failure, Object userContext) {
		System.out.println("onBufferFailure");
		failure.printStackTrace();
		BufferGenerator checker=(BufferGenerator)userContext;
		checker.end();
	}
	
}
