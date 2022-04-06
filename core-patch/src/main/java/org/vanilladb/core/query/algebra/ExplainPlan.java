package org.vanilladb.core.query.algebra;

import java.util.Set;

import org.vanilladb.core.query.algebra.materialize.TempTable;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;
import static org.vanilladb.core.sql.Type.VARCHAR;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;



public class ExplainPlan implements Plan {



	public static Histogram explainHistogram(Histogram hist, Set<String> fldNames) {
		Histogram expHist = new Histogram(fldNames);
		for (String fld : fldNames) {
			expHist.setBuckets(fld, hist.buckets(fld));
		}
		return expHist;
	}


	private Plan p;
	private Schema schema = new Schema();
	private Histogram hist;
	private Transaction tx;
	private TempTable expTempTable = null;
	private TableScan expTableScan = null;

	/**
	 * Creates a new explain node in the query tree.
	 * 
	 * @param p
	 *            the subquery
	 */
	public ExplainPlan(Plan p, Set<String> fldNames, Transaction tx) {
		this.p = p;
		this.tx = tx;
		for (String fldname : fldNames)
			schema.addField(fldname, VARCHAR(500));
		hist = explainHistogram(p.histogram(), fldNames);
		System.out.println("in explain plan ctor");
		System.out.println("pseudo");
		System.out.println(toString());
		expTempTable = new TempTable(schema, tx);
		expTableScan = (TableScan) expTempTable.open();
		expTableScan.insert();
		expTableScan.setVal("query-plan", (Constant) new VarcharConstant(toString()));
	}

	@Override
	public Scan open() {
		// Scan s = p.open();
		return new ExplainScan(expTableScan, schema.fields());
	}

	@Override
	public long blocksAccessed() {
		return p.blocksAccessed();
	}

	@Override
	public Schema schema() {
		return schema;
	}

	@Override
	public Histogram histogram() {
		
		return hist;
	}

	@Override
	public long recordsOutput() {
		
		return (long) histogram().recordsOutput();
	}

	@Override
	public String toString() {
		String c = p.toString();
		// String[] cs = c.split("\n");
		StringBuilder sb = new StringBuilder();
		sb.append("\n"); // add new line to make SQL console happy
		// sb.append("->");
		// sb.append("ExplainPlan (#blks=" + blocksAccessed() + ", #recs="
				// + recordsOutput() + ")\n");
		// for (String child : cs)
			// sb.append("\t").append(child).append("\n");
		// ;
		sb.append(c);
		sb.append("Actual #recs:");
		return sb.toString();
	}

}
