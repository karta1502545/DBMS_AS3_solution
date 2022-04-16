package org.vanilladb.core.query.algebra;

import java.util.Set;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.metadata.statistics.Histogram;

public class explainplan implements Plan{
	private Plan p;
	private Schema schema = new Schema();
	private Histogram hist;
	public explainplan(Plan p) {
		this.p = p;
		schema.addField("query-plan", org.vanilladb.core.sql.Type.VARCHAR(500));
	}
	public Scan open() {
		Scan s = p.open();
		return new explainscan(s,toString());
	}
	public Schema schema() {
		return schema;
	}
	public long blocksAccessed() {
		return p.blocksAccessed();
	}
	public Histogram histogram() {
		return hist;
	}
	public long recordsOutput() {
		return (long) histogram().recordsOutput();
	}
	public String toString() {
		String c = p.toString();
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		sb.append(c).append("\n");
		sb.append("Actual #recs: "+p.recordsOutput()+"\n");
		return sb.toString();
	}
}
