package naru.async.timer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import naru.async.Timer;
import naru.queuelet.Queuelet;
import naru.queuelet.QueueletContext;

/**
 * @author naru
 */
public class TimerManager implements Queuelet,Runnable{
	static private Logger logger=Logger.getLogger(TimerManager.class);
	//タイマーの粒度、これ以下のタイマー値を設定してもこの値と同値となる
	private static long timerInterval=1000;
	public static long INVALID_ID=-1;
	private static TimerManager timerManager=null;
	
	private static Map<Long,TimerEntry> timers=new HashMap<Long,TimerEntry>();
	private static TreeSet<TimerEntry> orderByTime=new TreeSet<TimerEntry>(TimerEntry.timerTimeComparator);
	private QueueletContext queueletContext;
	private boolean run;
	private Thread timerThread;
	
	public void init(QueueletContext context, Map param) {
		logger.info("TimerManager init");
		timerManager=this;
//		System.out.println("init timerManager.run:"+timerManager.run +":this:" + this+":timerManager:"+timerManager);
		queueletContext=context;
		String timerIntervalString=(String)param.get("timerInterval");
		if(timerIntervalString!=null){
			timerInterval=Long.parseLong(timerIntervalString);
		}
		timerThread=new Thread(this);
		timerThread.setName("timerThread");
		run=true;
		timerThread.start();
	}

	public boolean service(Object req) {
		if(!run){
			return true;
		}
		TimerEntry entry=(TimerEntry)req;
//		System.out.println("service:" +Thread.currentThread().getName()+":timerId:"+entry.getPoolId()+":this:"+this);
		entry.callback();//この中で例外をcatchしているので必ず復帰する
		return true;
	}

	public void term() {
		logger.info("TimerManager trem");
		try {
			synchronized(this){
				run=false;
//				timerManager.run=false;
//				System.out.println("term timerManager.run:"+timerManager.run +":this:" + this+":timerManager:"+timerManager);
//				new Throwable().printStackTrace();
				notifyAll();
			}
			timerThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		logger.debug("TimerManager trem phase 1");
		while(queueletContext.queueLength()>0){
			Object o=queueletContext.deque();
			service(o);
		}
		logger.debug("TimerManager trem phase 2");
		List<TimerEntry> entrys=selectTimeoutEntry(0);
		if(entrys!=null){
			for(TimerEntry entry:entrys){
				service(entry);
			}
		}
		logger.debug("TimerManager trem end");
	}
	
	private boolean enqueSafe(TimerEntry entry){
		try {
			//queueが停止処理中の場合、ここで例外となる
			//Exception in thread "timerThread" java.lang.IllegalStateException: Terminal:timer status error.status:3
			queueletContext.enque(entry);
		} catch (RuntimeException e) {
			logger.warn("TimerManger enque error.",e);
			return false;
		}
		return true;
	}
	
	public void run() {
		while(true){
			long nextWakeup=System.currentTimeMillis()+timerInterval;
			List<TimerEntry> entrys=selectTimeoutEntry(System.currentTimeMillis());
//			System.out.println("timer:entrys:"+entrys);
			if(entrys!=null){
				Iterator<TimerEntry> itr=entrys.iterator();
				while(itr.hasNext()){
					TimerEntry entry=itr.next();
//					System.out.println("enque:" +Thread.currentThread().getName()+":timerId:"+entry.getPoolId());
					if(!enqueSafe(entry)){
						logger.warn("fail to timer event."+entry);
					}
				}
			}
			try {
				synchronized(this){
					long timeout=nextWakeup-System.currentTimeMillis();
					if(timeout>0){
						wait(timeout);
					}
					if(run==false){
						break;
					}
				}
			} catch (InterruptedException e) {
				logger.warn("fail to wait",e);
			}
		}
	}
	
	private static synchronized List<TimerEntry> selectTimeoutEntry(long time){
//		logger.debug("time:"+time);
		List<TimerEntry> result=null;
		Iterator<TimerEntry> itr=orderByTime.iterator();
		while(itr.hasNext()){
			TimerEntry entry=itr.next();
			if(entry.getTime()>time){
				break;
			}
//			System.out.println("selectTimeoutEntry remove:timerId:"+entry.getPoolId());
			timers.remove(entry.getId());
			itr.remove();
			if(result==null){
				result=new ArrayList<TimerEntry>();
			}
			result.add(entry);
		}
		return result;
	}
	
	/**
	 * interval後にtimer.onTimeout(userContext)を呼び出す事を要求
	 * @return
	 */
	public static synchronized long setTimeout(long interval,Timer timer,Object userContext) {
//		System.out.println("setTimeout timerManager.run:"+timerManager.run);
		if(!timerManager.run){
			return -1;
		}
		long timoutTime=System.currentTimeMillis()+interval;
		TimerEntry entry=TimerEntry.getEntry();
		entry.setup(timoutTime, timer, userContext);
		long timerId=entry.getId();
		timers.put(timerId, entry);
		orderByTime.add(entry);
//		logger.debug("setTimeout interval:"+interval + ":userContext:" +userContext +":timerId:" +timerId);
		return timerId;
	}
	
	/**
	 * timerをキャンセルする
	 * 注）falseで復帰した後、onTimeoutが呼び出される可能性がある。
	 * @return cancelに成功した場合true
	 */
	public static synchronized boolean clearTimeout(long timerId) {
		logger.debug("clearTimeout timerId:"+timerId);
		TimerEntry entry=timers.remove(timerId);
		if(entry==null){
			logger.debug("clearTimeout return false.");
			return false;
		}
		orderByTime.remove(entry);
		entry.pool();
		return true;
	}
	
	/**
	 * setTimeout,clearTimeoutを利用してintervalTimerを実装
	 * @author naru
	 */

	private static class IntervalContext implements Timer{
		private long interval;
		private Timer timer;
		private Object userContext;
		private long timerId;
		private boolean isClearRecived=false;
		
		IntervalContext(long interval,Timer timer,Object userContext){
			this.interval=interval;
			this.timer=timer;
			this.userContext=userContext;
		}
		
		private void start(){
			timerId=setTimeout(interval, this, null);
		}
		
		public void stop(){
			synchronized(this){
				isClearRecived=true;
//				System.out.println("stop:" +Thread.currentThread().getName()+":timerId:"+timerId);
				if(clearTimeout(timerId)){
					return;
				}
				while(timerId>=0){
					try {
						wait(interval);
					} catch (InterruptedException ignore) {
					}
					break;
				}
			}
		}
		
		public void onTimer(Object dummyContext) {
			synchronized(this){
//				System.out.println("onTimer:" +Thread.currentThread().getName()+":timerId:"+timerId);
				timerId=-1;
				if(isClearRecived){
					notify();
					return;
				}
			}
			try{
				timer.onTimer(userContext);
			}catch(Throwable t){
			}
			synchronized(this){
				if(isClearRecived){
					timerId=-1;
					notify();
					return;
				}
				timerId=setTimeout(interval, this, null);
//				System.out.println("setTimeout:" +Thread.currentThread().getName()+":timerId:"+timerId);
			}
		}
	}
	
	/**
	 * interval後にtimer.onTimeout(userContext)を呼び出す事を要求
	 * @return
	 */
	public static Object setInterval(long interval,Timer timer,Object userContext) {
		IntervalContext context=new IntervalContext(interval,timer,userContext);
		context.start();
		return context;
	}
	
	public static void clearInterval(Object interval) {
		if(interval==null){
			return;
		}
		IntervalContext context=(IntervalContext)interval;
		context.stop();
	}
	
}
