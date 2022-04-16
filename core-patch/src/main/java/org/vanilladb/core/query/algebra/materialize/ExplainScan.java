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
package org.vanilladb.core.query.algebra.materialize;

import java.util.Arrays;
import java.util.List;

import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.RecordComparator;
import org.vanilladb.core.storage.record.RecordId;
import java.util.Collection;

/**
 * The Scan class for the <em>sort</em> operator.
 * 
 */
public class ExplainScan implements Scan {
	private Scan s;
	private Collection<String> fieldList;

	// [mod] add firstnext flag
	private Boolean firstNext;
	private String explainString;

	/**
	 * Creates a project scan having the specified underlying scan and field
	 * list.
	 * 
	 * @param s
	 *                  the underlying scan
	 * @param fieldList
	 *                  the list of field names
	 */
	public ExplainScan(Scan s, Collection<String> fieldList, String explainString) {
		this.s = s;
		this.fieldList = fieldList;
		this.firstNext = true;
		this.explainString = explainString;
	}

	@Override
	public void beforeFirst() {
		s.beforeFirst();
	}

	@Override
	public boolean next() {
		if(firstNext){
			firstNext = false;
			return true;
		}
		return false;
	}

	@Override
	public void close() {
		s.close();
	}

	@Override
	public Constant getVal(String fldName) {
		if (hasField(fldName))
			return new VarcharConstant(explainString);
		else
			throw new RuntimeException("field " + fldName + " not found.");
	}

	/**
	 * Returns true if the specified field is in the projection list.
	 * 
	 * @see Scan#hasField(java.lang.String)
	 */
	@Override
	public boolean hasField(String fldName) {
		return fieldList.contains(fldName);
	}
}
