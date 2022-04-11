package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;

public class explainscan implements Scan {
	private Scan s;
	private String ans;
	public explainscan(Scan s,String ans) {
		this.s = s;
		this.ans=ans;
	}
	public void beforeFirst() {
		s.beforeFirst();
	}
	public boolean next() {
		return s.next();
	}

	@Override
	public void close() {
		s.close();
	}
	public Constant getVal(String fldName) {
		return s.getVal(fldName);
	}
	public boolean hasField(String fldName) {
		return s.hasField(fldName);
	}

}
