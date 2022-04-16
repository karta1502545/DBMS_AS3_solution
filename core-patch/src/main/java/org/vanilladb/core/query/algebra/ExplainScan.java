package org.vanilladb.core.query.algebra;

import java.util.Collection;


import org.vanilladb.core.sql.Constant;

public class ExplainScan implements Scan {
	// private UpdateScan ups1 = null;
	private Scan s = null;
	private Collection<String> fieldList;

	public ExplainScan(Scan s, Collection<String> fieldList) {
		System.out.println("debug in explain scan ctor ");
		System.out.println("expscan ctor print fieldlist"+fieldList.toString());
		this.s = s;
		this.fieldList = fieldList;
	}

	@Override
	public Constant getVal(String fldName) {
		System.out.println("debug in explain scan get val: fldname="+fldName);
		if (hasField(fldName))
			return s.getVal(fldName);
		else
			throw new RuntimeException("field " + fldName + " not found.");
	}

	@Override
	public void beforeFirst() {
		s.beforeFirst();

	}

	@Override
	public boolean next() {
		
		return s.next();
	}

	@Override
	public void close() {
		s.close();

	}

	@Override
	public boolean hasField(String fldName) {
		
		return fieldList.contains(fldName);
	}

}
