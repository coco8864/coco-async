package naru.async.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolManager;
import naru.queuelet.Queuelet;
import naru.queuelet.QueueletContext;

public class IOManager implements Queuelet {
	private static Logger logger=Logger.getLogger(IOManager.class);
	private static QueueletContext queueletContext;
	private static SelectorContext selectors[];
	private static SelectorStastics stastics[];
	
	public static SelectorContext getSelectorContext(ChannelContext context){
		int index=(int)(context.getPoolId()%selectors.length);
		return selectors[index];
	}
	public static SelectorStastics[] getSelectorStasticses(){
		return stastics;
	}
	
	/**
	 * queuelet　terminalへのキューイング
	 * @author Naru
	 *
	 */
	public static void enqueue(Object obj){
		if(obj==null){
			return;
		}
		if(obj instanceof ChannelContext){
			//IOが完了するまでChannelContextが再利用されないようにする
			ChannelContext channelContext=(ChannelContext)obj;
			channelContext.ref();
			logger.debug("IOManager enqueue.cid:"+((ChannelContext)obj).getPoolId());
		}
		queueletContext.enque(obj);
	}
	
	private static String STOP_REQUEST="stop";
	public static void stop(){
		enqueue(STOP_REQUEST);
	}
	
	/* (非 Javadoc)
	 * @see naru.quelet.Quelet#init()
	 */
	public void init(QueueletContext context,Map param) {
		this.queueletContext=context;
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
		selectors=new SelectorContext[selectorCount];
		stastics=new SelectorStastics[selectorCount];
		try {
			for(int i=0;i<selectors.length;i++){
				selectors[i]=new SelectorContext(i,selectInterval);
				stastics[i]=selectors[i].getStastics();
				selectors[i].start(i);
			}
		} catch (IOException e) {
			logger.error("fail to new SelectorContext.",e);
			context.finish();
		}
	}

	/* (非 Javadoc)
	 * @see naru.quelet.Quelet#term()
	 */
	public void term() {
		for(int i=0;i<selectors.length;i++){
			selectors[i].stop();
		}
	}
	
	private boolean executeRead(ChannelContext context) {
		context.prepareIO(ChannelContext.IO.READING);
		ReadableByteChannel channel=(ReadableByteChannel)context.getChannel();
		ByteBuffer buffer=PoolManager.getBufferInstance();
		long length=0;
		Throwable failure=null;
		try {
			length=channel.read(buffer);
			buffer.flip();
			logger.debug("##executeRead length:"+length +":cid:"+context.getPoolId());
			//logger.debug("%%%%%%%%%%%%");
			//logger.debug(new String(buffer.array(),0,buffer.limit()));
			//logger.debug("%%%%%%%%%%%%");
		} catch (IOException e) {
			failure=e;
			logger.warn("fail to read.cid:"+context.getPoolId() +":channel:"+ channel,failure);
		} catch (Throwable e) {
			failure=e;
			logger.warn("fail to read Throwable.cid:"+context.getPoolId() +":channel:"+ channel,failure);
		}
		if(failure!=null){
			PoolManager.poolBufferInstance(buffer);
			context.disconnect();
			if( context.failure(failure) ){
				logger.warn("fail to read closable");
			}
			context.dump();
			return false;
		}
		
		if(length<=0){
			logger.debug("executeRead cid:"+context.getPoolId()+":length:"+length+":channel:"+channel+":buffer:"+buffer);
			context.dump();
			PoolManager.poolBufferInstance(buffer);
			context.disconnect();
//			context.doneClosed(true);
			return false;
		}else{
			context.doneRead(BuffersUtil.toByteBufferArray(buffer));
		}
		return true;
	}
	
	private boolean executeWrite(ChannelContext context) {
		Throwable failure=null;
		GatheringByteChannel channel=null;
		long length=0;
		ByteBuffer[] prepareBuffers=null;
		boolean isWriteExecute=true;
		try {
			prepareBuffers=context.prepareWrite();
			if(prepareBuffers==null){
				isWriteExecute=false;
				return false;//swap　outされていてすぐにbufferが用意できなかった。やり直し
			}
			channel=(GatheringByteChannel)context.getChannel();
			length=channel.write(prepareBuffers);
			logger.debug("##executeWrite length:"+length +":cid:"+context.getPoolId());
			//lengthが0の事があるらしい,buffersの長さが0に違いない,buffer再利用の問題か？
			if(logger.isDebugEnabled()){
				logger.debug("executeWrite."+length +":cid:"+context.getPoolId()+":channel:"+channel);
				if(length==0){
					for(int i=0;i<prepareBuffers.length;i++){
						logger.error("0 buffer.array:"+prepareBuffers[i].array());
					}
					logger.error("buffers length:"+BuffersUtil.remaining(prepareBuffers));
				}
			}
		} catch (IOException e) {
			logger.warn("fail to write.channel:"+channel+":cid:"+context.getPoolId(),e);
			failure=e;
		} catch (Throwable e) {
			logger.warn("fail to write. Throwable channel:"+channel+":cid:"+context.getPoolId(),e);
			failure=e;
		}finally{
			if(failure==null){
				if(isWriteExecute){
					context.doneWrite(prepareBuffers,length);
				}
			}else{
				if(prepareBuffers!=null){
					PoolManager.poolArrayInstance(prepareBuffers);
				}
				context.disconnect();
				if( context.failure(failure) ){
					logger.warn("fail to write closable");
//					context.doneClosed(false);
				}
			}
		}
		return true;
	}
	
	private boolean executeConnect(ChannelContext context) {
		context.prepareIO(ChannelContext.IO.CONNECTING);
		try {
			context.finishConnect();
			context.queueuSelect();//queueselectに置く
			return true;
		} catch (IOException e) {
			logger.warn("connect error", e);
			context.failure(e);
		}
		return false;
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
		ChannelContext context=(ChannelContext)req;
		try{
			ChannelContext.IO io=context.getIoStatus();
			logger.debug("IOManager service.cid:"+context.getPoolId()+":io:"+io);
			switch(io){
			case CONNECTABLE:
				executeConnect(context);
				break;
			case READABLE:
				executeRead(context);
				break;
			case WRITABLE:
				executeWrite(context);
				break;
			case CLOSEABLE:
				context.prepareIO(ChannelContext.IO.CLOSEING);
				context.doneClosed(false);//要求によりcloseする場合
				break;
			default:
				logger.error("IOManager.service error.io:"+io+":cid:"+context.getPoolId());
			}
			return false;
		}finally{
			//IOが完了するまでChannelContextが再利用されないようにする
			context.unref();
		}
	}
	
}
