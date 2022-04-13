package org.vanilladb.core.query.algebra;

import java.util.Collection;
import java.util.List;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.record.RecordFile;

/**
 * The scan class corresponding to the <em>project</em> relational algebra
 * operator. All methods except hasField delegate their work to the underlying
 * scan.
 */
public class ExplainScan implements Scan {
	private Schema schema = new Schema();
	private RecordFile rf;

	/**
	 * Creates a project scan having the specified underlying scan and field
	 * list.
	 * 
	 * @param s
	 *            the underlying scan
	 * @param fieldList
	 *            the list of field names
	 */
	public ExplainScan(Schema schema,List<String> explain) {
		this.schema = schema;
		rf.setVal("query-plan", new VarcharConstant(explain.toString()));
	}

	@Override
	public void beforeFirst() {
		rf.beforeFirst();
	}

	@Override
	public boolean next() {
		return rf.next();
	}

	@Override
	public void close() {
		rf.close();
	}

	@Override
	public Constant getVal(String fldName) {
		return rf.getVal("query-plan");
	}

	/**
	 * Returns true if the specified field is in the projection list.
	 * 
	 * @see Scan#hasField(java.lang.String)
	 */
	@Override
	public boolean hasField(String fldName) {
		return schema.hasField(fldName);
	}
}
