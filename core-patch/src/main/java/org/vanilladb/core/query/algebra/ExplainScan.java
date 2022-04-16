package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;

public class ExplainScan implements Scan {
	
	private String result;
	private Schema schema;
	private int numRecords;
	private boolean beforeFirst;
	
	public ExplainScan(Scan scan, Schema schema, String explain) {
		this.result = "\n" + explain;
		this.schema = schema;
		scan.beforeFirst();
		
		while (scan.next()) {
			numRecords++;
		}
		
		scan.close();
		this.result = result + "\nActual #recs: " + numRecords;
		beforeFirst = true;
	}

	@Override
	public Constant getVal(String fldName) {
		if (fldName.equals("query-plan")) {
			return new VarcharConstant(result);
		} else
			throw new RuntimeException("field " + fldName + " not found.");
	}

	@Override
	public void beforeFirst() {
		beforeFirst = true;
	}

	@Override
	public boolean next() {
		if (beforeFirst) {
			beforeFirst = false;
			return true;
		} else {
			return false;
		}
	}

	@Override
	public void close() { }

	@Override
	public boolean hasField(String fldName) {
		return schema.hasField(fldName);
	}

}
