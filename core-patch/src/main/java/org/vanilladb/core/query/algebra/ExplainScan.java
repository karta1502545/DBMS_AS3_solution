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
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;

/**
 * The scan class corresponding to the <em>project</em> relational algebra
 * operator. All methods except hasField delegate their work to the underlying
 * scan.
 */
public class ExplainScan implements Scan {
	private Scan s;
	private Schema schema;
	private String result;
	private boolean isBF;

	/**
	 * Creates a project scan having the specified underlying scan and field
	 * list.
	 * 
	 * @param s
	 *            the underlying scan
	 * @param schema
	 * 				schema
	 * @param result
	 * 				output result
	 */
	public ExplainScan(Scan s, Schema schema, String str) {
		this.s = s;
		this.schema = schema;

		int counter = 0;
		s.beforeFirst();
		while(s.next()) {
			counter++;
		}
		s.close();
		str = str + "Actual #recs: " + String.valueOf(counter);

		this.result = "\n" + str;
	}

	@Override
	public void beforeFirst() {
		isBF = true;
	}

	@Override
	public boolean next() {
		if(isBF) {
			isBF = false;
			return true;
		}
		else{
			return false;
		}
	}

	@Override
	public void close() {
	}

	@Override
	public Constant getVal(String fldName) {
		if (fldName.equals("query-plan")) {
			return new VarcharConstant(this.result);
		} else
			throw new RuntimeException("field " + fldName + " not found.");
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
