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
package org.vanilladb.core.query.planner;

import org.junit.*;
import org.vanilladb.core.query.algebra.*;
import org.vanilladb.core.query.parse.*;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.tx.Transaction;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.*;

public class ExplainScanTest {
	private static Logger logger = Logger
			.getLogger(ExplainScanTest.class.getName());
	private ExplainTree et;
	private Transaction tx;
	
	@BeforeClass
	public static void init() {
		ServerInit.init(ExplainScanTest.class);
		ServerInit.loadTestbed();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN EXPLAIN SCAN TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH EXPLAIN SCAN TEST");
	}
	
	
	@Before
	public void createExplainTree() {
		tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		et = new ExplainTree(
				"ProjectPlan",
				"",
				2,
				1
		);
		ExplainTree gb = new ExplainTree(
				"GroupByPlan",
				":",
				2,
				1
		);
		ExplainTree sp = new ExplainTree(
				"SortPlan",
				"",
				2,
				10
		);
		ExplainTree selp = new ExplainTree(
				"SelectPlan",
				"pred:(d_w_id=w_id)",
				22,
				10
		);
		ExplainTree pp = new ExplainTree(
				"ProductPlan",
				"",
				22,
				10
		);
		ExplainTree et3_clone = new ExplainTree(
				"[HAHA type depth = 2]",
				"HAHA detail",
				1024,
				4
		);
		ExplainTree tp1  = new ExplainTree(
				"TablePlan",
				"on (warehouse)",
				2,
				1
		);
		ExplainTree tp2  = new ExplainTree(
				"TablePlan",
				"on (district)",
				2,
				10
		);
		pp.addChild(tp1);
		pp.addChild(tp2);
		selp.addChild(et3_clone);
		selp.addChild(pp);
		sp.addChild(selp);
		gb.addChild(sp);
		et.addChild(gb);
	}
	
	@After
	public void finishTx() {
		tx.commit();
		tx = null;
	}

	@Test
	public void test() {
		String qry = "select sid, sname, majorid from student, dept "
				+ "where majorid=did and dname='dept0'";
		Parser psr = new Parser(qry);
		QueryData qd = psr.queryCommand();
		Plan p = new BasicQueryPlanner().createPlan(qd, tx);
		ExplainPlan ep = new ExplainPlan(
				p
		);
		Scan es = new ExplainScan(p.open(), et);
		System.out.println(es.getVal("query-plan"));
	}
}
