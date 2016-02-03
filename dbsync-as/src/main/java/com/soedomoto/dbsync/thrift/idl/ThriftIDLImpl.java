package com.soedomoto.dbsync.thrift.idl;

import com.facebook.swift.generator.swift2thrift.Main;

public class ThriftIDLImpl implements ThriftIDL {

	@Override
	public byte[] getFile() throws Exception {
		Main.main(
			"-package com.soedomoto.dbsync.dbms " + 
			"-namespace java com.soedomoto.dbsync.dbms -namespace py dbsync " + 
			"Column DBMS");
		
		return null;
	}

}
