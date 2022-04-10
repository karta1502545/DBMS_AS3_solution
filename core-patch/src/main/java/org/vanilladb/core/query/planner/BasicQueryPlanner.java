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
//AS3: Sheep Modified
 package org.vanilladb.core.query.planner;

import java.util.ArrayList;
import java.util.List;


import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.ProductPlan;
import org.vanilladb.core.query.algebra.ProjectPlan;
import org.vanilladb.core.query.algebra.SelectPlan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.ExplainPlan;
import org.vanilladb.core.query.algebra.materialize.GroupByPlan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The simplest, most naive query planner possible.
 */
public class BasicQueryPlanner implements QueryPlanner {

	/**
	 * Creates a query plan as follows. It first takes the product of all tables
	 * and views; it then selects on the predicate; and finally it projects on
	 * the field list.
	 */
	@Override
	public Plan createPlan(QueryData data, Transaction tx) {
		List<String> TableList = new ArrayList<String>();
		List<String> ProductList = new ArrayList<String>();
		List<String> SelectList = new ArrayList<String>();
		List<String> GroupByList = new ArrayList<String>();
		List<String> SortInGroupList = new ArrayList<String>();
		List<String> ProjectList = new ArrayList<String>();
		List<String> SortList = new ArrayList<String>();
		
		String temp;
		long final_rec=0; 
		// Step 1: Create a plan for each mentioned table or view
		List<Plan> plans = new ArrayList<Plan>();
		for (String tblname : data.tables()) {
			String viewdef = VanillaDb.catalogMgr().getViewDef(tblname, tx);
			if (viewdef != null)
				plans.add(VanillaDb.newPlanner().createQueryPlan(viewdef, tx));
			else {
				Plan p = new TablePlan(tblname, tx);
				plans.add(p);
				if (data.isExplain()){
					temp = "->TablePlan on ("+tblname+") (#blks="+String.valueOf(p.blocksAccessed())+", #recs="+String.valueOf(p.recordsOutput())+")\n";
					TableList.add(temp);
				}
			}
		}
		// Step 2: Create the product of all table plans
		Plan p = plans.remove(0);
		for (Plan nextplan : plans) {
			p = new ProductPlan(p, nextplan);
			if (data.isExplain()){
				temp = "->ProductPlan (#blks="+String.valueOf(p.blocksAccessed())+", #recs="+String.valueOf(p.recordsOutput())+")\n";
				ProductList.add(temp);
			}
		}
		// Step 3: Add a selection plan for the predicate
		p = new SelectPlan(p, data.pred());
		if (data.isExplain()){
			temp = "->SelectPlan pred:("+data.pred().toString()+") (#blks="+String.valueOf(p.blocksAccessed())+", #recs="+String.valueOf(p.recordsOutput())+")\n";
			SelectList.add(temp);
		}
		// Step 4: Add a group-by plan if specified
		if (data.groupFields() != null) {
			p = new GroupByPlan(p, data.groupFields(), data.aggregationFn(), tx);
			if (data.isExplain()){
				temp = "->GroupByPlan (#blks="+String.valueOf(p.blocksAccessed())+", #recs="+String.valueOf(p.recordsOutput())+")\n";
				GroupByList.add(temp);
				if (p.sortInfoInGroup()!=""){
					SortInGroupList.add(p.sortInfoInGroup());
				}
			}
		}
		// Step 5: Project onto the specified fields
		p = new ProjectPlan(p, data.projectFields());
		if (data.isExplain()){
			final_rec = p.recordsOutput();
			temp = "->ProjectPlan (#blks="+String.valueOf(p.blocksAccessed())+", #recs="+String.valueOf(p.recordsOutput())+")\n";
			ProjectList.add(temp);
		}
		// Step 6: Add a sort plan if specified
		if (data.sortFields() != null) {
			p = new SortPlan(p, data.sortFields(), data.sortDirections(), tx);
			if (data.isExplain()){
				temp = "->SortPlan (#blks="+String.valueOf(p.blocksAccessed())+", #recs="+String.valueOf(p.recordsOutput())+")\n";
				SortList.add(temp);
			}
		}
		//Step 7: Add an explain plan if specified
		int space_num=0; 
		if(data.isExplain()) {
			String info = "----------------------------------------------------------------\n";
			if(!SortList.isEmpty()) {
				info += addInfo(SortList, space_num);
				space_num++;
			}
			if(!ProjectList.isEmpty()) {
				info += addInfo(ProjectList, space_num);
				space_num++;
			}
			if(!GroupByList.isEmpty()) {
				info += addInfo(GroupByList, space_num);
				space_num++;
			}
			if(!SortInGroupList.isEmpty()) {
				info += addInfo(SortInGroupList, space_num);
				space_num++;
			}
			if(!SelectList.isEmpty()) {
				info += addInfo(SelectList, space_num);
				space_num++;
			}
			if(!ProductList.isEmpty()) {
				info += addInfo(ProductList, space_num);
				space_num++;
			}
			if(!TableList.isEmpty()) {
				info += addInfo(TableList, space_num);
				space_num++;
			}
			info += "Actual #recs: " + String.valueOf(final_rec) + "\n";
			p = new ExplainPlan(p, info);
		}
		return p;
	}
	private String addInfo(List<String> CurList, int space_num){
		String temp="";
		for (String s: CurList){
			for (int j=0; j<space_num; j++) temp += "   ";
			temp += s;
		}
		return temp;
	}
}
