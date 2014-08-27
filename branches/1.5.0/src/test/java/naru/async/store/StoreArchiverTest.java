package naru.async.store;


import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import naru.async.store.StoreArchiver;
import naru.async.store.StoreManager;
import naru.queuelet.test.TestBase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class StoreArchiverTest extends TestBase{
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
	
	private static String fileName="archiverTest.zip";
	
	@Test
	public void testSaveToArchiver() throws Throwable{
		callTest("qtestSaveToArchiver");
	}
	public void qtestSaveToArchiver() throws Throwable{
		Set<Long> ids=StoreManager.listPersistenceStoreId();
		File file=new File(fileName);
		StoreArchiver.toArchive(ids, file);
	}
	
	@Test
	public void testRestoreToArchiver() throws Throwable{
		callTest("qtestRestoreToArchiver");
	}
	public void qtestRestoreToArchiver() throws Throwable{
		File file=new File(fileName);
		Map<String,String> result=StoreArchiver.fromArchive(file);
		Iterator<String> nameItr=result.keySet().iterator();
		while(nameItr.hasNext()){
			String name=nameItr.next();
			String digest=result.get(name);
			System.out.println("name:"+name+ ":digest:"+digest+":size:"+StoreManager.getStoreLength(digest));
			StoreManager.unref(digest);
		}
	}
	
}
