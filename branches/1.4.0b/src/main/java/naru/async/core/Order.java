package naru.async.core;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import naru.async.ChannelHandler;
import naru.async.ChannelStastics;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;

public class Order extends PoolBase{
	private static Logger logger=Logger.getLogger(Order.class);
	
	enum OrderType{
		non,
		select,
		accept,
		connect,
		read,
		write,
		close,
		finish
	}
	
	private ChannelHandler handler;
	private OrderType orderType;//read,write,connect,accept,close
	private Object userContext;
	private Object[] userContexts;
	private ByteBuffer buffers[];
	private Throwable failure;
	private boolean isTimeout;
	private boolean isCloseOrder;//強制closeのため完了したOrder
	private long writeStartOffset;
	private long writeEndOffset;
	private long timeoutTime;
	
	public long getTimeoutTime() {
		return timeoutTime;
	}
	public void setTimeoutTime(long timeoutTime){
		this.timeoutTime=timeoutTime;
	}
	public static Order create(ChannelHandler handler,OrderType orderType,Object userContext){
		return create(handler,orderType,userContext,null);
	}
	public static Order create(ChannelHandler handler,OrderType orderType,Object userContext,ByteBuffer[] buffers){
		Order order=(Order)PoolManager.getInstance(Order.class);
		logger.debug("Order#create:"+handler.getPoolId()+":"+orderType+ ":" +order);
		order.setHandler(handler);
		order.orderType=orderType;
		order.userContext=userContext;
		order.buffers=buffers;
		return order;
	}
	
	public void recycle() {
		setHandler(null);
		orderType=OrderType.non;
		userContext=null;
		userContexts=null;
		failure=null;
		isTimeout=isCloseOrder=false;
		if(buffers!=null){
			PoolManager.poolBufferInstance(buffers);
			buffers=null;
		}
	}
	
	private void setHandler(ChannelHandler handler){
		if(handler!=null){
			handler.ref();
		}
		if(this.handler!=null){
//			logger.debug("handler.unref():"+this.handler);
			this.handler.unref();
		}
		this.handler=handler;
	}
	
	ChannelHandler getHandler(){
		return handler;
	}
	
	private void callbackFailurer(ChannelStastics stastics){
		switch(orderType){
		case read:
			stastics.onReadFailure();
			handler.onReadFailure(userContext, failure);
			break;
		case write:
			stastics.onWriteFailure();
			if(userContexts!=null){
				handler.onWriteFailure(userContexts,failure);
			}else{
				handler.onWriteFailure(new Object[]{userContext},failure);
			}
			break;
		case accept:
			stastics.onAcceptFailure();
			handler.onAcceptFailure(userContext,failure);
			break;
		case connect:
			stastics.onConnectFailure();
			handler.onConnectFailure(userContext,failure);
			break;
		case close:
			stastics.onCloseFailure();
			handler.onCloseFailure(userContext,failure);
			break;
		}
	}
	
	private void callbackTimeout(ChannelStastics stastics){
		switch(orderType){
		case read:
			stastics.onReadTimeout();
			handler.onReadTimeout(userContext);
			break;
		case write:
			stastics.onWriteTimeout();
			if(userContexts!=null){
				handler.onWriteTimeout(userContexts);
			}else{
				handler.onWriteTimeout(new Object[]{userContext});
			}
			break;
		case connect:
			stastics.onConnectTimeout();
			handler.onConnectTimeout(userContext);
			break;
		}
	}
	
	private void callbackClosed(ChannelStastics stastics){
		switch(orderType){
		case read:
			stastics.onReadClosed();
			handler.onReadClosed(userContext);
			break;
		case write:
			stastics.onWriteClosed();
			if(userContexts!=null){
				handler.onWriteClosed(userContexts);
			}else{
				handler.onWriteClosed(new Object[]{userContext});
			}
			break;
		case accept:
			stastics.onAcceptClosed();
			handler.onAcceptClosed(userContext);
			break;
		case connect:
			stastics.onConnectClosed();
			handler.onConnectClosed(userContext);
			break;
		case close:
			stastics.onCloseClosed();
			handler.onCloseClosed(userContext);
			break;
		}
	}
	
	private void internalCallback(ChannelStastics stastics){
		if(failure!=null){
			callbackFailurer(stastics);
		}else if(isTimeout){
			callbackTimeout(stastics);
		}else if(isCloseOrder){
			callbackClosed(stastics);
		}else{
			switch(orderType){
			case read:
				stastics.onRead();
				handler.onRead(userContext, popBuffers());
				break;
			case write:
				stastics.onWritten();
				handler.onWritten(userContext);
				break;
			case select:
				stastics.onAcceptable();
				handler.onAcceptable(userContext);
				break;
			case accept:
				stastics.onAccepted();
				handler.onAccepted(userContext);
				break;
			case connect:
				stastics.onConnected();
				handler.onConnected(userContext);
				break;
			case close:
				stastics.onCloseClosed();
				handler.onCloseClosed(userContext);
				break;
			case finish:
				stastics.onFinished();
				handler.onFinished();
				//finishedを通知したため、handlerからcontextを切り離す
				//handler.setAttribute(SelectorContext.ATTR_ACCEPTED_CONTEXT, null);
				break;
			}
		}
	}
	
	public boolean isFinish(){
		return orderType==OrderType.finish;
	}
	
	public void callback(ChannelStastics ststics){
		logger.debug("callback."+orderType + ":" + handler);
		if(handler==null){
			logger.error("Illegal order.this:"+this,new Exception());
			return;
		}
		try{
			internalCallback(ststics);
		}catch(Throwable t){
			//ここに来たときhandlerは、foward後かもしれない
			logger.warn("callback return throwable.",t);
		}finally{
			logger.debug("callbacked.cid:"+handler.getChannelId()+":type:"+orderType);
			unref(true);//orderは通知したら寿命が切れる,orderは、handlerを所有しているのでorderの開放と共にhandlerの参照は減算される
		}
	}

	public OrderType getOrderType() {
		return orderType;
	}

	public void setOrderType(OrderType orderType) {
		this.orderType = orderType;
	}

	public ByteBuffer[] popBuffers() {
		ByteBuffer[] tmpBuffer=buffers;
		buffers=null;
		return tmpBuffer;
	}

	public void setBuffers(ByteBuffer[] buffers) {
		this.buffers = buffers;
	}

	public Object getUserContext() {
		return userContext;
	}
	public void setUserCountexts(Object[] userContexts){
		this.userContexts=userContexts;
	}

	public void setFailure(Throwable failure) {
		this.failure = failure;
	}

	public void timeout(){
		isTimeout=true;
	}
	
	public void closeOrder(){
		isCloseOrder=true;
	}
	
	public long getWriteEndOffset() {
		return writeEndOffset;
	}
	public void setWriteEndOffset(long writeEndOffset) {
		this.writeEndOffset = writeEndOffset;
	}
	public void setWriteStartOffset(long writeStartOffset) {
		this.writeStartOffset = writeStartOffset;
	}
}
