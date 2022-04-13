package org.vanilladb.core.query.algebra;

import static org.vanilladb.core.sql.Type.VARCHAR;

import java.util.Set;

import org.vanilladb.core.query.algebra.materialize.TempTable;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;

public class ExplainPlan implements Plan {
	
	private Plan p;
	private Schema schema = new Schema();
	private Transaction tx;
	private Constant c;
	
	public ExplainPlan(Plan p, Set<String> fldNames, Transaction tx) {
		this.p = p;
		for (String fldname : fldNames)
			schema.add(fldname, p.schema());
		schema.addField("query-plan", VARCHAR(500));
		this.tx = tx;
		c = new VarcharConstant(this.toString());
	}
	
	@Override
	public Scan open() {
		Schema sch = schema();
		TempTable temp = new TempTable(sch, tx);
		Scan src = p.open();
		UpdateScan dest = temp.open();
		src.beforeFirst();
		while (src.next()) {
			dest.insert();
			for (String fldname : sch.fields()) {
				if (fldname == "query-plan")
					dest.setVal(fldname, c);
				else
					dest.setVal(fldname, src.getVal(fldname));
			}
		}
		src.close();
		dest.beforeFirst();
		return dest;
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

	public String toString() {
		String c = p.toString();
		String[] cs = c.split("\n");
		StringBuilder sb = new StringBuilder();
		sb.append("->");
		sb.append(p.getClass().getName() + ": (#blks=" + blocksAccessed() + ", #recs="
				+ recordsOutput() + ")\n");
		for (String child : cs)
			sb.append("\t").append(child).append("\n");
		;
		return sb.toString();
	}
}
