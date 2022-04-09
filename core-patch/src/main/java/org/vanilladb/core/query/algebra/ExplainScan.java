package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.sql.Type;

public class ExplainScan implements Scan{
	//explain will not output record file
	private RecordFile rf;
	private String result;
	private Schema schema;
	private Scan s;
	private int numRecs;
	private String explain;
	private boolean IsbeforeFirst;
	
	
	public ExplainScan(Scan s,long blks,Schema schema,String p) {
		//this.s=p.open();
		this.explain=p.toString();
		s.beforeFirst();
		while (s.next())
			numRecs++;
		s.close();
		this.result = result + "\nActual #recs: " + numRecs;
		IsbeforeFirst = true;
	}
	@Override
	public void beforeFirst() {
		IsbeforeFirst=true;
	}

	@Override
	public boolean next() {
		if (IsbeforeFirst) {
			IsbeforeFirst=!IsbeforeFirst;
			return true;
		} else {
			return false;
		}
		
	}

	@Override
	public void close() {
		//rf.close();
	}

	/**
	 * Returns the value of the specified field, as a Constant.
	 * 
	 * @see Scan#getVal(java.lang.String)
	 */
	@Override
	public Constant getVal(String fldName) {
		//return rf.getVal(fldName);
		if(fldName.equals("query-plan"))
			return new VarcharConstant(result);
		else
			throw new RuntimeException("is not query-plan");
	}

	@Override
	public boolean hasField(String fldName) {
		return schema.hasField(fldName);
	}
}
