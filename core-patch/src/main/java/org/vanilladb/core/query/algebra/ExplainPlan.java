// [AS3] ExplainPlan
package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import static org.vanilladb.core.sql.Type.VARCHAR;

public class ExplainPlan implements Plan {
	private Plan p;
	private Schema schema = new Schema();
	private String planTree = "\n";
	
	public ExplainPlan(Plan p) {
		this.p = p;
		schema.addField("query-plan", VARCHAR(500));
		planTree += "->Explain Query Test\n";
		planTree += "\t->Explain Query Test 1";
	}

	@Override
	public Scan open() {
		Scan s = p.open();
		return new ExplainScan(s, planTree);
	}

	@Override
	public long blocksAccessed() {
		return 0;
	}

	@Override
	public Schema schema() {
		return schema;
	}

	@Override
	public Histogram histogram() {
		return null;
	}

	@Override
	public long recordsOutput() {
		return 0;
	}
}
