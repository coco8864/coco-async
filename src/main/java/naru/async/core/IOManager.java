package naru.async.core;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;

import naru.async.Log;
import naru.async.pool.LocalPoolManager;
import naru.queuelet.Queuelet;
import naru.queuelet.QueueletContext;

public class IOManager implements Queuelet {
	private static Logger logger=Logger.getLogger(IOManager.class);
	private static IOManager instance;
	private QueueletContext queueletContext;
	private SelectorHandler selectors[];
	private SelectorStastics stastics[];
	private boolean isTcpNoDelay=true;
	private int soLingerTime=-1;
	private boolean isReuseAddress=true;
	private boolean isAcceptThread=true;//accept専用のthreadを起こすか否か
	
	static boolean isTcpNoDelay(){
		return instance.isTcpNoDelay;
	}
	
	static int getSoLingerTime(){
		return instance.soLingerTime;
	}

	static boolean isReuseAddress(){
		return instance.isReuseAddress;
	}
	
	static boolean isAcceptThread(){
		return instance.isAcceptThread;
	}
	
	public static SelectorHandler getSelectorContext(ChannelContext context){
		int index=(int)(context.getPoolId()%instance.selectors.length);
		return instance.selectors[index];
	}
	public static SelectorStastics[] getSelectorStasticses(){
		return instance.stastics;
	}
	
	/**
	 * queuelet　terminalへのキューイング
	 * @author Naru
	 *
	 */
	public static void enqueue(Object req){
		if(req==null){
			return;
		}
		if(req!=STOP_REQUEST){
			ChannelContext ctx=((ChannelIO)req).getContext();
			ctx.ref();
			Log.debug(logger,"enqueue.cid:",ctx.getPoolId(),":type:",req);
		}
		instance.queueletContext.enque(req);
	}
	
	private static String STOP_REQUEST="stop";
	public static void stop(){
		enqueue(STOP_REQUEST);
	}
	
	/* (非 Javadoc)
	 * @see naru.quelet.Quelet#init()
	 */
	public void init(QueueletContext context,Map param) {
		instance=this;
		queueletContext=context;
		String selectorCountParam=(String)param.get("selectorCount");
		int selectorCount=4;
		if(selectorCountParam!=null){
			selectorCount=Integer.parseInt(selectorCountParam);
		}
		logger.info("selectorCount:"+selectorCount);
		long selectInterval=60000;//selectがタイムアウトする最大時間、実際にはtimeout時間で調整する
		String selectIntervalParam=(String)param.get("selectInterval");
		if(selectIntervalParam!=null){
			selectInterval=Long.parseLong(selectIntervalParam);
		}
		logger.info("selectInterval:"+selectInterval);
		selectors=new SelectorHandler[selectorCount];
		stastics=new SelectorStastics[selectorCount];
		try {
			for(int i=0;i<selectors.length;i++){
				selectors[i]=new SelectorHandler(i,selectInterval);
				stastics[i]=selectors[i].getStastics();
				selectors[i].start(i);
			}
		} catch (IOException e) {
			logger.error("fail to new SelectorContext.",e);
			context.finish();
		}
		
		isTcpNoDelay=!"false".equalsIgnoreCase((String)param.get("isTcpNoDelay"));
		isReuseAddress=!"false".equalsIgnoreCase((String)param.get("isReuseAddress"));
		isAcceptThread=!"false".equalsIgnoreCase((String)param.get("isAcceptThread"));
		this.soLingerTime=-1;
		String soLingerTime=(String)param.get("soLingerTime");
		if(soLingerTime!=null){
			this.soLingerTime=Integer.parseInt(soLingerTime);
		}
		logger.info("isTcpNoDelay:"+isTcpNoDelay);
		logger.info("soLingerTime:"+this.soLingerTime);
		logger.info("isReuseAddress:"+isReuseAddress);
		logger.info("isAcceptThread:"+isAcceptThread);
		
	}

	/* (非 Javadoc)
	 * @see naru.quelet.Quelet#term()
	 */
	public void term() {
		for(int i=0;i<selectors.length;i++){
			selectors[i].stop();
		}
	}
	
	public boolean service(Object req) {
		if(req==STOP_REQUEST){
			logger.info("recive stop request");
			for(int i=0;i<selectors.length;i++){
				selectors[i].stop();
				selectors[i].wakeup();
			}
			return false;
		}
		ChannelIO channelIO=(ChannelIO)req;
		try{
			channelIO.doIo();
			return false;
		}finally{
			//IOが完了するまでChannelContextが再利用されないようにする
			channelIO.getContext().unref();
			LocalPoolManager.refresh();
		}
	}
	
}
