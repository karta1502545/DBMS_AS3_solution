package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;

public class ExplainScan implements Scan {
	Scan s;
	
	public ExplainScan(Scan s) {
		this.s = s;
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
	public Constant getVal(String fldName) {
		try {
			return s.getVal(fldName);
		}
		catch (Exception ex) {
			throw new RuntimeException("field " + fldName + " not found.");
		}
	}

	/**
	 * Returns true if the specified field is in the projection list.
	 * 
	 * @see Scan#hasField(java.lang.String)
	 */
	@Override
	public boolean hasField(String fldName) {
		return s.hasField(fldName);
	}
}
