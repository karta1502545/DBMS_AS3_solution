package org.vanilladb.core.query.algebra;

import java.util.ArrayList;
import java.util.List;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.TableNotFoundException;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.metadata.statistics.TableStatInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;

public class ExplainPlan implements Plan {
	private List<String> explain = new ArrayList<String>();
	private Schema schema = new Schema();

	public ExplainPlan(List<String> explain) {
		schema.addField("query-plan", Type.VARCHAR);
		this.explain = explain;
	}
	
	@Override
	public Scan open() {
		return new ExplainScan(schema, explain);
	}
	
	@Override
	public Schema schema() {
		return schema;
	}

	@Override
	public long blocksAccessed() {
		// TODO Auto-generated method stub
		return 1;
	}
	
	private Histogram hist;
	@Override
	public Histogram histogram() {
		//hist = projectHistogram(new Explain.histogram(), "query-plan");
		return hist;
	}

	@Override
	public long recordsOutput() {
		// TODO Auto-generated method stub
		return 1;
	}



}
