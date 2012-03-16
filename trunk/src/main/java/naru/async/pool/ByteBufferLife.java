package naru.async.pool;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

public class ByteBufferLife extends ReferenceLife {
	static private Logger logger=Logger.getLogger(ByteBufferLife.class);
	/* ByteBuffer���L�[��ByteBufferLife����������ꍇ�ɗ��p */
	static class SearchKey extends ByteBufferLife{
		private ByteBufferLife hit;//��������
		private ByteBuffer byteBuffer;
		SearchKey(){
			super(null, null);
		}
		public void setByteBuffer(ByteBuffer byteBuffer) {
			this.byteBuffer = byteBuffer;
		}
		public ByteBufferLife getHit() {
			return hit;
		}
		@Override
		public int hashCode() {
			return System.identityHashCode(byteBuffer);
		}
		@Override
		public boolean equals(Object obj) {
			if(obj instanceof ByteBufferLife){
				if(((ByteBufferLife)obj).get()==byteBuffer){
					hit=(ByteBufferLife)obj;
					return true;
				}
			}
			return false;
		}
	}

	private ByteArrayLife arrayLife;
	private int hashCode;
	
	public ByteBufferLife(Object referent,ByteArrayLife arrayLife) {
		super(referent);
		this.hashCode=System.identityHashCode(referent);
		this.arrayLife=arrayLife;
		//pool�͐ݒ肵�Ȃ�
	}
	
	public byte[] getArray(){
		return (byte[])arrayLife.get();
	}

	void gcInstance() {
		arrayLife.gcByteBufferLife(this);
	}

	@Override
	public int hashCode() {
		return hashCode;
	}
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof SearchKey){
			SearchKey searchKey=(SearchKey)obj;
			if(searchKey.byteBuffer==this.get()){
				searchKey.hit=this;
				return true;
			}
		}
		return this==obj;
	}
	
	@Override
	public String toString(){
		return "ByteBufferLife@" +hashCode;
	}
	
}
