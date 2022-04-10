package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import static org.vanilladb.core.sql.Type.VARCHAR;


public class ExplainPlan implements Plan {
	private Plan p;
	private Schema schema = new Schema();
	private String explain_msg;

	public ExplainPlan(Plan p) {
		this.p = p;
		schema.addField("query-plan", VARCHAR(500));
//		schema.addAll(p.schema());

		explain_msg = toString();
	}

	@Override
	public Scan open() {
		Scan s = p.open();
		int actual_recs = 0;
		s.beforeFirst();
		while (s.next())
			actual_recs++;
		s.close();
		explain_msg += "Actual #recs: " + actual_recs + "\n";
		return new ExplainScan(s, explain_msg);
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
		return p.histogram();
	}

	@Override
	public long recordsOutput() {
		return (long) histogram().recordsOutput();
	}

	@Override
	public String toString() {
		String c = p.toString();
		return "\n"+c ;
	}
}
