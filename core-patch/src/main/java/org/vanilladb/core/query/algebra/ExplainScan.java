package org.vanilladb.core.query.algebra;

import java.util.Collection;
import static org.vanilladb.core.sql.Type.VARCHAR;

import org.vanilladb.core.sql.Constant;

public class ExplainScan implements Scan {

	private Scan s;
	private Collection<String> fieldList;
	private String explain;
	private boolean first;
	
	public ExplainScan(Scan s, Collection<String> fieldList, String explain) {
		this.s = s;
		this.fieldList = fieldList;
		this.explain = explain;
		this.first = true;
	}
	
	@Override
	public Constant getVal(String fldName) {
		if (hasField(fldName))
			return Constant.newInstance(VARCHAR, explain.getBytes());
		else
			throw new RuntimeException("field " + fldName + " not found.");
	}

	@Override
	public void beforeFirst() {
		s.beforeFirst();

	}

	@Override
	public boolean next() {
		if(first) {
			first = false;
			return true;
		}
		return false;
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
