package naru.async.core;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

import naru.async.ChannelHandler;
import naru.async.core.Order.OrderType;
import naru.async.core.SelectOperator.State;
import naru.async.pool.BuffersUtil;

import org.apache.log4j.Logger;
/**
 * 
 * ChannelContext���ێ�����Order���Ǘ�
 * �r���́AChannelContext���Ŋl��
 * @author naru
 *
 */
public class OrderOperator {
	private static Logger logger=Logger.getLogger(OrderOperator.class);
	private static final long CLOSE_INTERVAL=10000;
	private static class DummyHandler extends ChannelHandler{
		@Override
		public void onFinished() {
		}
	}
	private static final DummyHandler DUMMY_HANDLER=new DummyHandler();
	
	private boolean isFinished;//finish�@Order�����s�������ۂ�
	private Order acceptOrder;
	private Order connectOrder;
	private Order readOrder;
	private LinkedList<Order> writeOrders=new LinkedList<Order>();
	private Order closeOrder;
	private Throwable failure;
	
	private ChannelContext context;

	public OrderOperator(ChannelContext context){
		this.context=context;
	}
	
	/* callback�֘A */
	//private static ThreadLocal<ChannelContext> currentContext=new ThreadLocal<ChannelContext>();
	private LinkedList<Order> callbackOrders=new LinkedList<Order>();
	private boolean inCallback=false;
	public synchronized void queueCallback(Order order){
		callbackOrders.add(order);
		logger.debug("queueCallback cid:"+context.getPoolId() +":size:"+callbackOrders.size()+":"+order.getOrderType());
		if(inCallback==false){
			inCallback=true;
			DispatchManager.enqueue(this);
		}
	}
	
	synchronized boolean checkAndCallbackFinish(){
		if(isFinished){
			return false;
		}
		if(orderCount()!=0){
			return false;
		}
		synchronized(context){
			if(!context.getSelectOperator().isClose()){
				return false;
			}
			if(!context.getWriteOperator().isClose()){
				return false;
			}
		}
		isFinished=true;
		queueCallback(Order.create(context.getHandler(), OrderType.finish, null));
		return true;
	}
	
	private synchronized Order popOrder(){
		logger.debug("callback cid:"+context.getPoolId() +":size:"+callbackOrders.size());
		if(callbackOrders.size()==0){
			if(checkAndCallbackFinish()==false){
				inCallback=false;
				return null;
			}
		}
		Order order=(Order)callbackOrders.remove(0);
		return order;
	}
	
	/**
	 * dispatch worker����Ăяo�����A���d�ł͑��s���Ȃ�
	 */
	public void callback(){
		Order order=null;
		boolean isFinishCallback=false;
		ChannelHandler finishHandler=null;
		try{
			while(true){
				order=popOrder();
				if(order.isFinish()){
					isFinishCallback=true;
					finishHandler=order.getHandler();
				}
				try{
					order.callback(context.getChannelStastics());
				}catch(Throwable t){
					logger.warn("callback return Throwable",t);
					//������ؒf���ꂽ�Ƃ��ď���
					//doneClosed(true);
				}
			}
		}finally{
			//currentContext.set(null);
			if(isFinishCallback==false){
				return;
			}
			logger.debug("callback isFinishCallback=true.cid:"+context.getPoolId());
			//if(finishHandler!=handler){
			//	logger.warn("finish callback finishHandler:"+finishHandler);
			//	logger.warn("finish callback handler:"+handler);
			//}
			finishHandler.unref();
			context.setHandler(null);
			context.unref();
		}
	}
	
	public void dump(){
		dump(logger);
	}
	public void dump(Logger logger){
		//writeOrders�́A���̂��_���v�����ConcurrentModificationException�ƂȂ�B
		StringBuffer sb=new StringBuffer();
		sb.append("$writeOrders.size:");
		sb.append(writeOrders.size());
		sb.append(":acceptOrder:");
		sb.append(acceptOrder);
		sb.append(":connectOrder:");
		sb.append(connectOrder);
		sb.append(":readOrder:");
		sb.append(readOrder);
		sb.append(":closeOrder:");
		sb.append(closeOrder);
		sb.append(":failure:");
		sb.append(failure);
		logger.debug(sb.toString());
	}
	public void recycle() {
		acceptOrder=null;
		connectOrder=null;
		readOrder=null;
		writeOrders.clear();
		closeOrder=null;
		failure=null;
	}
	
	boolean isCloseOrder(){
		return closeOrder!=null;
	}
	
	public boolean isReadOrder(){
		return readOrder!=null;
	}
	
	public boolean isWriteOrder(){
		return writeOrders.size()!=0;
	}
	public boolean isConnectOrder(){
		return connectOrder!=null;
	}
	
	public static final long CHECK_TIMEOUT=-1;
	
	long checkConnectTimeout(long now){
		if(connectOrder==null){
			return CHECK_TIMEOUT;
		}
		long timeoutTime=connectOrder.getTimeoutTime();
		if(now<timeoutTime){
			return timeoutTime;
		}
		return CHECK_TIMEOUT;
	}
	
	long checkReadTimeout(long now){
		if(readOrder==null){
			return CHECK_TIMEOUT;
		}
		long timeoutTime=readOrder.getTimeoutTime();
		if(now<timeoutTime){
			return timeoutTime;
		}
		return CHECK_TIMEOUT;
	}
	
	long checkWriteTimeout(long now){
		long timeoutTime=Long.MAX_VALUE;
		for(Order order:writeOrders){
			long time=order.getTimeoutTime();
			if(timeoutTime>time){
				timeoutTime=time;
			}
		}
		if(now<timeoutTime){
			return timeoutTime;
		}
		return CHECK_TIMEOUT;
	}
	
	public int orderCount(){
		int count=0;
		if(acceptOrder!=null){
			count++;
		}
		if(connectOrder!=null){
			count++;
		}
		if(readOrder!=null){
			count++;
		}
		count+=writeOrders.size();
		if(closeOrder!=null){
			count++;
		}
		return count;
	}
	
	/**
	 * �SOrder��failure�ʒm
	 */
	int failure(Throwable failure){
		int count=0;
		this.failure=failure;
		if(acceptOrder!=null){
			acceptOrder.setFailure(failure);
			queueCallback(acceptOrder);
			count++;
			acceptOrder=null;
		}
		if(connectOrder!=null){
			connectOrder.setFailure(failure);
			queueCallback(connectOrder);
			count++;
			connectOrder=null;
		}
		if(readOrder!=null){
			readOrder.setFailure(failure);
			queueCallback(readOrder);
			count++;
			readOrder=null;
		}
		for(Order writeOrder:writeOrders){
			writeOrder.setFailure(failure);
			queueCallback(writeOrder);
			count++;
		}
		writeOrders.clear();
		if(closeOrder!=null){
			closeOrder.setFailure(failure);
			queueCallback(closeOrder);
			count++;
			closeOrder=null;
		}
		return count;
	}
	
	
	/**
	 * �SOrder��failure�ʒm
	 */
	private int closeOrder(){
		int count=0;
		if(acceptOrder!=null){
			acceptOrder.closeOrder();
			queueCallback(acceptOrder);
			count++;
			acceptOrder=null;
		}
		if(connectOrder!=null){
			connectOrder.closeOrder();
			queueCallback(connectOrder);
			count++;
			connectOrder=null;
		}
		if(readOrder!=null){
			readOrder.closeOrder();
			queueCallback(readOrder);
			count++;
			readOrder=null;
		}
		for(Order writeOrder:writeOrders){
			writeOrder.closeOrder();
			queueCallback(writeOrder);
			count++;
		}
		writeOrders.clear();
		if(closeOrder!=null){
			closeOrder.closeOrder();
			queueCallback(closeOrder);
			count++;
			closeOrder=null;
		}
		return count;
	}
	
	
	/**
	 * �w�肳�ꂽorder��timeout�ʒm
	 * connect��write��timeout��failure�[�Ƃ��Ĉ���
	 * read�͕����\
	 */
	public void timeout(OrderType orderType){
		switch(orderType){
		case connect:
		case write:
			failure(new Exception("timeout:"+orderType));
			break;
		case read:
			if(readOrder!=null){
				readOrder.timeout();
				queueCallback(readOrder);
				readOrder=null;
			}
			break;
		}
	}
	
	/**
	 * �w�肳�ꂽorder��closed�ʒm
	 */
	public int doneClose(boolean isAsyncClose){
		int count=0;
		if(closeOrder!=null){
			queueCallback(closeOrder);
			count++;
			closeOrder=null;
		}
		if(isAsyncClose){
			/*�@����n�ł͂��΂炭�҂�0����M����͂�,0����M�������ԑ҂� */
			if(readOrder!=null){
				readOrder=Order.create(DUMMY_HANDLER, OrderType.read, null);
			}
			readOrder.setTimeoutTime(System.currentTimeMillis()+CLOSE_INTERVAL);
		}else{
			closeOrder();
		}
		return count;
	}
	
	public boolean doneRead(ArrayList<ByteBuffer> buffers){
		if(readOrder==null || buffers.size()==0){
			return false;
		}
		Order order=readOrder;
		readOrder=null;
		ByteBuffer[] bufs=BuffersUtil.toByteBufferArray(buffers);
		buffers.clear();
		order.setBuffers(bufs);
		queueCallback(order);
		return true;
	}
	
	public boolean doneWrite(long totalWriteLength){
		if(writeOrders.size()==0){
			return false;
		}
		Iterator<Order> itr=writeOrders.iterator();
		while(itr.hasNext()){
			Order order=itr.next();
			if(order.getWriteEndOffset()>totalWriteLength){
				break;
			}
			itr.remove();
			logger.debug("doneWrite queueCallback cid:"+context.getPoolId()+":userContext:"+order.getUserContext());
			queueCallback(order);
		}
		logger.debug("doneWrite cid:"+context.getPoolId()+" writeOrderCount:"+writeOrders.size());
		return true;
	}
	
	public boolean doneConnect(){
		if(connectOrder==null){
			return false;
		}
		queueCallback(connectOrder);
		connectOrder=null;
		return true;
	}
	
	
	boolean acceptOrder(Object userContext){
		if(acceptOrder!=null){
			return false;
		}
		acceptOrder=Order.create(context.getHandler(), OrderType.accept, userContext);
		context.getSelectOperator().queueSelect(State.accepting);
		return true;
	}
	
	boolean connectOrder(Object userContext,long timeout){
		if(connectOrder!=null){
			return false;
		}
		connectOrder=Order.create(context.getHandler(), OrderType.connect, userContext);
		if(timeout>0){
			connectOrder.setTimeoutTime(System.currentTimeMillis()+timeout);
		}
		if(context.isConnected()){//���ł�connect���������Ă����ꍇ
			queueCallback(connectOrder);
			connectOrder=null;
			context.getSelectOperator().queueSelect(State.reading);
		}else{
			context.getSelectOperator().queueSelect(State.connecting);
		}
		return true;
	}
	
	boolean readOrder(Object userContext,long timeoutTime){
		if(readOrder!=null){
			return false;
		}
		readOrder=Order.create(context.getHandler(), OrderType.read, userContext);
		readOrder.setTimeoutTime(timeoutTime);
		if( context.getSelectOperator().asyncRead(readOrder) ){
			readOrder=null;
		}
		return true;
	}
	
	boolean writeOrder(Object userContext,ByteBuffer[] buffers,long asyncWriteStartOffset,long length,long timeoutTime){
		if( context.getWriteOperator().asyncWrite(buffers)==false){
			return false;
		}
		Order order=Order.create(context.getHandler(), OrderType.write, userContext);
		order.setWriteStartOffset(asyncWriteStartOffset);
		order.setWriteEndOffset(asyncWriteStartOffset+length);
		order.setTimeoutTime(timeoutTime);
		writeOrders.add(order);
		return true;
	}
	
	boolean closeOrder(Object userContext){
		if(closeOrder!=null){
			return false;
		}
		if( context.getWriteOperator().asyncClose()==false){
			return false;
		}
		closeOrder=Order.create(context.getHandler(), OrderType.close, userContext);
		return true;
	}
}
