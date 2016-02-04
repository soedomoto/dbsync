package com.soedomoto.dbsync.thrift;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

public class SerializeTools {

	public static byte[] serialize(Object obj) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		try {
			out = new ObjectOutputStream(bos);
			out.writeObject(obj);
			byte[] yourBytes = bos.toByteArray();
			
			return yourBytes;
		} finally {
			try {
				if (out != null) {
					out.close();
				}
			} catch (IOException ex) {}
			
			try {
				bos.close();
			} catch (IOException ex) {}
		}
	}
	
	public static Object deserialize(byte[] yourBytes) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bis = null;
		ObjectInput in = null;
		Object o = null;
		try {
			bis = new ByteArrayInputStream(yourBytes);
			in = new ObjectInputStream(bis);
			o = in.readObject(); 
		} finally {
			try {
				bis.close();
			} catch (IOException ex) {}
			
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {}
		}
		
		return o;
	}
	
}
