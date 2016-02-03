package com.soedomoto.dbsync.thrift.idl;

import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;

@ThriftService
public interface ThriftIDL {
	@ThriftMethod
	public byte[] getFile() throws Exception;
}
