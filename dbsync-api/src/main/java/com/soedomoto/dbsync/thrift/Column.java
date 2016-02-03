package com.soedomoto.dbsync.thrift;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public class Column {
	private final String name;
	private final String type;
	private final Boolean nullable;
	private final String defaultValue;
	
	@ThriftConstructor
	public Column(String name, String type, Boolean nullable, String defaultValue) {
		this.name = name;
		this.type = type;
		this.nullable = nullable;
		this.defaultValue = defaultValue;
	}

	@ThriftField(1)
	public String getName() {
		return name;
	}

	@ThriftField(2)
	public String getType() {
		return type;
	}

	@ThriftField(3)
	public Boolean getNullable() {
		return nullable;
	}

	@ThriftField(4)
	public String getDefaultValue() {
		return defaultValue;
	}
	
}
