package naru.async.timer;

import static org.junit.Assert.*;

import java.io.IOException;

import naru.async.Timer;
import naru.async.timer.TimerManager;
import naru.queuelet.test.TestBase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TimerTest extends TestBase implements Timer{
	@BeforeClass
	public static void beforClass() throws IOException {
		System.out.println("Queuelet container start");
		setupContainer("TimerTest");
	}
	@AfterClass
	public static void afterClass() {
		stopContainer();
		System.out.println("Queuelet container stop");
	}
	
	@Test
	public void testDummy() throws Throwable {
		callTest("qtestDummy");
	}
	public void qtestDummy() {
		String test = "test";
		long start = System.currentTimeMillis();
		System.out.println("start:" + start);
		TimerManager.setTimeout(500, this, test);
		Object o = deque("testQueue");
		long diff = System.currentTimeMillis() - start;
		assertSame("context check", test, o);
		// inerval=100‚Æ‚µ‚½ê‡A100ms‚ÌŒë·‚ª‚ ‚é
		System.out.println("diff:" + diff);
//		assertTrue("time check", (diff >= 500 && diff < 600));
	}
	public void onTimer(Object userContext) {
		System.out.println("onTimer:" + System.currentTimeMillis());
		enque(userContext, "testQueue");
	}
	
	@Test
	public void testTimer() throws Throwable {
		callTest("qtestTimer");
	}
	
	public void qtestTimer(){
		String test="test";
		long start=System.currentTimeMillis();
		System.out.println("start:"+start);
		TimerManager.setTimeout(500, this, test);
		Object o=deque("testQueue");
		long diff=System.currentTimeMillis()-start;
		assertSame("context check", test, o);
		//inerval=100‚Æ‚µ‚½ê‡A100ms‚ÌŒë·‚ª‚ ‚é
		System.out.println("diff:"+diff);
		assertTrue("time check",(diff>=500 && diff<600) );
	}

	@Test
	public void testCancel() throws Throwable {
		callTest("qtestCancel");
	}
	public void qtestCancel(){
		String test1="test1";
		String test2="test2";
		long start=System.currentTimeMillis();
		long timerId1=TimerManager.setTimeout(100, this, test1);
		long timerId2=TimerManager.setTimeout(200, this, test2);
		assertTrue("clear check",TimerManager.clearTimeout(timerId1));
		Object o=deque("testQueue");
		long diff=System.currentTimeMillis()-start;
		assertSame("context check", test2, o);
		//inerval=1000‚Æ‚µ‚½ê‡A1000ms‚ÌŒë·‚ª‚ ‚é
		System.out.println("diff:"+diff);
		assertTrue("time check",(diff>=200 && diff<300) );
		assertFalse("clear check",TimerManager.clearTimeout(timerId2));
	}
}
