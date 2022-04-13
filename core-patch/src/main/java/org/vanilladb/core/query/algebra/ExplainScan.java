package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.record.RecordId;

public class ExplainScan implements UpdateScan {
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

	@Override
	public void setVal(String fldname, Constant val) {
		UpdateScan us = (UpdateScan) s;
		us.setVal(fldname, val);
	}

	@Override
	public void delete() {
		UpdateScan us = (UpdateScan) s;
		us.delete();
	}

	@Override
	public void insert() {
		UpdateScan us = (UpdateScan) s;
		us.insert();
	}

	@Override
	public RecordId getRecordId() {
		UpdateScan us = (UpdateScan) s;
		return us.getRecordId();
	}

	@Override
	public void moveToRecordId(RecordId rid) {
		UpdateScan us = (UpdateScan) s;
		us.moveToRecordId(rid);
	}
}
