/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
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
package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.metadata.statistics.Histogram;

public class ExplainPlan implements Plan {

	private final Plan p;
	private final Schema schema;

	public ExplainPlan(Plan p) {
		this.p = p;
		schema = new Schema();
		schema.addField("query-plan", Type.VARCHAR(500));
	}

	/**
	 * Creates an explain scan
	 *
	 * @return a explain scan
	 */
	@Override
	public Scan open() {
		return new ExplainScan(p, schema);
	}

	/**
	 * Returns an estimate of the number of block accesses that will occur when
	 * the scan is read to completion.
	 *
	 * @return the estimated number of block accesses
	 */
	@Override
	public long blocksAccessed() {
		return p.blocksAccessed();
	}

	/**
	 * Returns the schema of the query.
	 *
	 * @return the query's schema
	 */
	@Override
	public Schema schema() {
		return schema;
	}

	/**
	 * Returns the histogram that approximates the join distribution of the
	 * field values of query results.
	 *
	 * @return the histogram
	 */
	@Override
	public Histogram histogram() {
		return p.histogram(); // TODO: is this correct?
	}

	/**
	 * Returns an estimate of the number of records in the query's output table.
	 *
	 * @return the estimated number of output records
	 */
	@Override
	public long recordsOutput() {
		return 1;
	}

	/**
	 * Returns explanation of the plan and its sub-plan
	 *
	 * @param level the indention level
	 * @return explain
	 */
	@Override
	public String generateExplanation(int level) {
//		String explanation = String.format("ExplainPlan (#blks=%d, #recs=%d)%n", blocksAccessed(), recordsOutput());
//		explanation += p.generateExplanation(level + 1);
		return p.generateExplanation(level);
	}

}
