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
		int count = 0;
		s.beforeFirst();
		while (s.next()) count++;
		s.close();
		explain_msg += "Actual #recs: " + count + "\n";
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
