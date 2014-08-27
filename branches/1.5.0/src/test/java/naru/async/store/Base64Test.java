package naru.async.store;


import org.junit.Test;

public class Base64Test {
	@Test
	public void test0() throws Throwable{
		for(int i=0;i<100;i++){
			String digest=DataUtil.digest(("xxxx"+i).getBytes());
			System.out.println("digest:" +digest);
		}
	}

}
