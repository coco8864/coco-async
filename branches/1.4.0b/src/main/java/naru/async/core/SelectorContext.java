package naru.async.core;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.apache.log4j.Logger;

import naru.async.ChannelHandler;
import naru.async.pool.PoolManager;

public class SelectorContext implements Runnable {
	static private Logger logger=Logger.getLogger(SelectorContext.class);
	private int id;
	
	private long selectInterval;
	private SelectorStastics stastics;
	private Selector selector;
	
	private Thread selectorThread;
	private LinkedList contexts=new LinkedList();
	private boolean isWakeup;
	private boolean isRun;
	
	public int getId(){
		return id;
	}
	SelectorContext(int id,long selectInterval) throws IOException{
		this.id=id;
		this.selectInterval=selectInterval;
		this.selector = Selector.open();
		this.stastics=new SelectorStastics(id,selectInterval);
	}
	
	SelectorStastics getStastics(){
		return stastics;
	}
	
	public void queueSelect(ChannelContext context){
		synchronized(contexts){
			stastics.inQueue();
			context.ref();
			contexts.add(context);
		}
	}
	public void wakeup(){
		if(isWakeup==false){
			//複数スレッドから同時に呼び出された場合、selector.wakeup()が複数呼び出される
			//可能性があるが容認（synchronizedしてまで守る必要なし)
			isWakeup=true;
			selector.wakeup();
//			logger.info("wakeup this:"+this);
		}
	}
	
	public SelectionKey keyFor(SelectableChannel channel){
		return channel.keyFor(selector);
	}
	
	public  SelectionKey register(SelectableChannel channel,int ops,Object att) throws ClosedChannelException{
		return channel.register(selector, ops, att);
	}
	
	/**
	 * 
	 * @param nextWakeup 次のタイムアウト時刻で一番近い時刻
	 * @return
	 */
	private void closeAll(){
		Iterator itr=selector.keys().iterator();
		while(itr.hasNext()){
			SelectionKey key=(SelectionKey)itr.next();
			if(key.isValid()==false){
				continue;
			}
			ChannelContext context=(ChannelContext)key.attachment();
			context.queueIO(ChannelContext.IO.CLOSEABLE);
		}
	}
	
	/**
	 * 
	 * @param nextWakeup 次のタイムアウト時刻で一番近い時刻
	 * @return
	 */
	private long selectAll(){
		long timeoutTime=Long.MAX_VALUE;
		Set nextContexts=new HashSet();
		Set<SelectionKey> keys=selector.keys();
		Iterator<SelectionKey> itr=keys.iterator();
		while(itr.hasNext()){
			SelectionKey key=itr.next();
			if(key.isValid()==false){
				continue;
			}
			ChannelContext context=(ChannelContext)key.attachment();
			if(context.select()){
				long time=context.getNextSelectWakeUp();
				if(time<timeoutTime){
					timeoutTime=time;
				}
			}else{//参加したかったが、参加させられなかった。
				nextContexts.add(context);
			}
		}
		while(true){
			ChannelContext context;
			synchronized(contexts){
				if(contexts.size()==0){
					break;
				}
				context=(ChannelContext)contexts.remove(0);
			}
			logger.debug("selectAll cid:"+context.getPoolId());
			if(nextContexts.contains(context)){
				//一回のselectで同じcontextのselectを複数回呼ぶのを防ぐ、弊害はないとは思うが
				context.unref();
				continue;
			}
			if(context.select()){
				long time=context.getNextSelectWakeUp();
				if(time<timeoutTime){
					timeoutTime=time;
				}
				context.unref();
			}else{//参加したかったが、参加させられなかった。
				nextContexts.add(context);
			}
		}
		int nextContextsCount=nextContexts.size();
		if(nextContextsCount!=0){
			if(logger.isDebugEnabled()){
				logger.debug("retry select.size:"+nextContexts.size());
				Iterator citr=nextContexts.iterator();
				while(citr.hasNext()){
					ChannelContext context=(ChannelContext)citr.next();
					logger.debug("id:"+context.getPoolId());
				}
			}
			synchronized(contexts){
				contexts.addAll(nextContexts);
			}
			//次selectがすぐに目覚める事を期待
			selector.wakeup();
//			logger.info("wakeup2 this:"+this);
			
		}
		return timeoutTime;
	}
	
	public static String ATTR_ACCEPTED_CONTEXT="naru.async.acceptedContext";
	
	private void dispatch() {
		// セレクトされた SelectionKeyオブジェクトをまとめて取得する
		Iterator keyIterator = selector.selectedKeys().iterator();
		while (keyIterator.hasNext()) {
			SelectionKey key = (SelectionKey) keyIterator.next();
			keyIterator.remove();//ずっと選択されたままになる事を防ぐ
			
			ChannelContext context=(ChannelContext)key.attachment();
			SelectableChannel selectableChannel=key.channel();
			SocketChannel socketChannel=null;
			if(selectableChannel instanceof SocketChannel){
				socketChannel=(SocketChannel)selectableChannel;
			}
			// セレクトされた SelectionKey の状態に応じて処理を決める
			if(!key.isValid()){
				logger.warn("dispatch key is aleardy canceled");
			}else if (key.isAcceptable()) {//ここからjava.nio.channels.CancelledKeyExceptionが発生する？
				ServerSocketChannel serverSocketChannel =(ServerSocketChannel) selectableChannel;
				try {
					socketChannel = serverSocketChannel.accept();
					Socket s=socketChannel.socket();
					if( !context.acceptable() ){
						logger.debug("refuse socketChannel:"+socketChannel);
						s.close();//接続拒否
						stastics.acceptRefuse();
						continue;
					}
					logger.debug("isAcceptable socketChannel:"+socketChannel);
				} catch (IOException e) {
					logger.error("fail to accept.",e);
					continue;//どうしようもない
				}
				//ユーザオブジェクトを獲得する
				ChannelHandler handler=(ChannelHandler)PoolManager.getInstance(context.getAcceptClass());
				ChannelContext acceptContext=ChannelContext.create(handler, socketChannel);
				
				handler.setHandlerAttribute(ATTR_ACCEPTED_CONTEXT, acceptContext);
				Object userAcceptContext=context.getAcceptUserContext();
				acceptContext.accepted(userAcceptContext);
				//2013/12/8 acceptContextとりあえず読んでみる(for perfomance)
				//acceptContext.queueuSelect();
				stastics.read();
				//acceptContext.setIoStatus(ChannelContext.IO.SELECT);
				acceptContext.queueIO(ChannelContext.IO.READABLE);
			} else if (key.isReadable()) {//READを優先的に判断
//				context.readable(true);
				stastics.read();
				context.getReadChannel().readable();
			}else if(key.isWritable()){
//				context.writable(true);
				stastics.write();
				context.getWriteChannel().writable();
			}else if(key.isConnectable()){
				// 接続可能になった場合
				stastics.connect();
				context.getReadChannel().connectable();
			}
		}
	}

	private static long MIN_INTERVAL_TIME=10;
	private static int MIN_INTERVAL_COUNT=16;
	/**
	 * リクエスト待ち、このメソッドはブロックします。
	 * リクエスト到着は、ChannelEventに通知します。
	 * 
	 * @throws IOException
	 */
	private void waitForRequest() throws IOException {
		isRun=true;
		isWakeup=false;//若干余分にwakeupが呼び出されるのは許容する
		long nextWakeup=System.currentTimeMillis()+selectInterval;//次にtimeoutする直近時刻
		//激しくwakeupされると寝る仕組み,10ms以下で16回loop...チューニング要素
		long lastWakeup=System.currentTimeMillis();
		int minCount=0;
		while (true) {
			long now=System.currentTimeMillis();
			if( (lastWakeup-now)<MIN_INTERVAL_TIME ){
				minCount++;
//				System.out.println("minCount:"+minCount);
				if(minCount>=MIN_INTERVAL_COUNT){
					try {
						stastics.sleep();
						Thread.sleep(MIN_INTERVAL_TIME);
					} catch (InterruptedException ignore) {
					}
					now=System.currentTimeMillis();
					minCount=0;
				}
			}else{
				minCount=0;
			}
			long interval=nextWakeup-now;
			if(interval>selectInterval){
				interval=selectInterval;
			}
			logger.debug("select in.interval:"+interval);
			stastics.loop();
			int selectCount=0;
			if(interval>0){
				lastWakeup=System.currentTimeMillis();
//				System.out.println(Thread.currentThread().getName() + ":interval:"+interval);
//				logger.info("select in.this:"+this+":interval:"+interval);
				Set<SelectionKey> keys=selector.keys();
				stastics.setSelectCount(keys.size());
				selectCount=selector.select(interval);
				isWakeup=false;//排他してないので、若干余分にwakeupが呼び出されるのは許容する
//				System.out.println(Thread.currentThread().getName() + ":out:"+interval);
				if(selectCount<0){
					logger.info("select out break.selectCount:"+selectCount+":this:"+this);
					break;
				}else if(selectCount!=0){
					minCount=0;//イベントが発生してwakeupされたならカウンタをクリア
				}
			}
			/* 選択されたチャネルを処理する */
//			logger.info("select out.selectCount:"+selectCount+":this:"+this);
			//nextWakeup=System.currentTimeMillis()+selectInterval;
			dispatch();
			
			/* selectモードを調整する(close timeout　変更) */
			nextWakeup=selectAll();
			
			/* 停止要求されている場合は、停止 */
			if( isRun==false ){
				closeAll();
				selector.close();
				break;
			}
		}
	}
	
	public void run() {
		try {
			waitForRequest();
			logger.info("SelectorContext nomal end.stastics:"+stastics.getLoopCount() +":"+ stastics.getSleepCount());
		} catch (IOException e) {
			logger.error("SelectorContext listener IOException end.",e);
		} catch (Throwable e) {
			logger.error("SelectorContext listener Throwable end.",e);
		}
	}
	
	public void start(int index){
		isRun=true;
		selectorThread=new Thread(this);
		selectorThread.setName("selector-" + index);
		selectorThread.start();
	}
	
	public void stop(){
		isRun=false;
		selector.wakeup();
		try {
			selectorThread.join();
		} catch (InterruptedException e) {
			logger.error("fail to join",e);
		}
	}
}
