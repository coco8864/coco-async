package naru.async.pool;

import org.apache.log4j.Logger;

public class ByteBufferLife extends ReferenceLife {
	static private Logger logger=Logger.getLogger(ByteBufferLife.class);

	private ByteArrayLife arrayLife;
	
	public ByteBufferLife(Object referent,ByteArrayLife arrayLife) {
		super(referent);
		this.arrayLife=arrayLife;
		//poolは設定しない
	}
	
	public byte[] getArray(){
		return (byte[])arrayLife.get();
	}

	void gcInstance() {
		arrayLife.gcByteBufferLife(this);
	}
}
