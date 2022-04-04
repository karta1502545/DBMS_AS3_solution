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

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;

public class ExplainScan implements Scan {

	private final Scan s;
	private final Schema schema;
	private final String explanation;
	private int count;

	public ExplainScan(Plan p, Schema schema) {
		this.s = p.open();
		this.schema = schema;
		this.count = -1;

		int count = 0;
		s.beforeFirst();
		while (s.next()) {
			count++;
		}
		this.explanation = String.format("%n") + p.generateExplanation(0) + String.format("%nActual #recs: %d", count);
	}

	/**
	 * Positions the scan before its first record.
	 */
	@Override
	public void beforeFirst() {
		s.beforeFirst();
		count = 0;
	}

	/**
	 * Moves the scan to the next record.
	 *
	 * @return false if there is no next record
	 */
	@Override
	public boolean next() {
		if (count == -1) {
			throw new IllegalStateException("You must call beforeFirst() before iterating the scan");
		}

		return count++ < 1;
	}

	/**
	 * Closes the scan and its sub-scans
	 */
	@Override
	public void close() {
		s.close();
	}

	/**
	 * Returns true if the scan has the specified field.
	 *
	 * @param fldName the name of the field
	 * @return true if the scan has that field
	 */
	@Override
	public boolean hasField(String fldName) {
		return schema.hasField(fldName);
	}

	/**
	 * Returns the {@link Constant value} of the specified field.
	 *
	 * @param fldName the name of the field
	 * @return the value of that field
	 */
	@Override
	public Constant getVal(String fldName) {
		if (hasField(fldName)) {
			return new VarcharConstant(explanation);
		} else {
			throw new RuntimeException("field " + fldName + " not found.");
		}
	}
}
