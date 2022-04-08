/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.query.algebra.materialize;

import static org.vanilladb.core.sql.RecordComparator.DIR_ASC;

import java.util.ArrayList;
import java.util.List;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.query.algebra.multibuffer.BufferNeeds;
import org.vanilladb.core.sql.RecordComparator;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.record.RecordFormatter;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The {@link Plan} class for the <em>sort</em> operator.
 */
public class ExplainPlan implements Plan {
	private Plan p;
	private RecordComparator comp;
	private Schema schema;

	/**
	 * Creates a explain plan for the specified query.
	 * 
	 * @param p
	 */
	public ExplainPlan(Plan p) {
		this.p = p;
		schema = new Schema();
	}

	/**
	 * This method is where most of the action is. Up to 2 sorted temporary
	 * tables are created, and are passed into SortScan for final merging.
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		Scan src = p.open();
		schema.addField("query-plan", Type.VARCHAR(500));
		// System.out.format("%s", schema.fields());
		return new ExplainScan(src, schema.fields(), toString());
	}

	/**
	 * Returns the number of blocks in the sorted table, which is the same as it
	 * would be in a materialized table. It does <em>not</em> include the
	 * one-time cost of materializing and sorting the records.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		// does not include the one-time cost of sorting
		// Plan mp = new MaterializePlan(p); // not opened; just for analysis
		return p.blocksAccessed();
	}

	/**
	 * Returns the schema of the sorted table, which is the same as in the
	 * underlying query.
	 * 
	 * @see Plan#schema()
	 */
	@Override
	public Schema schema() {
		return schema;
	}

	/**
	 * Returns the histogram that approximates the join distribution of the
	 * field values of query results.
	 * 
	 * @see Plan#histogram()
	 */
	@Override
	public Histogram histogram() {
		return p.histogram();
	}

	@Override
	public long recordsOutput() {
		return p.recordsOutput();
	}

	//[mod]
	public String toString(){
		String c = p.toString();
		int cnt = 0;
		// String[] cs = c.split("\n");
		StringBuilder sb = new StringBuilder();
		// sb.append("query-plan" + "\n" + "----------------------------------------------------------------" + "\n");
		sb.append("\n" + c);
		// sb.append("->");
		// sb.append("ExplainPlan (#blks=" + blocksAccessed() + ", #recs="
		// 		+ recordsOutput() + ")\n");
		// for (String child : cs)
		// 	sb.append("\t").append(child).append("\n");
		// ;
		Scan s = p.open();
		s.beforeFirst();
		while(s.next()){
			cnt++;
		}
		// sb.append("Actual #recs: " + recordsOutput());
		sb.append("Actual #recs: " + cnt);
		return sb.toString();
	} 

}
