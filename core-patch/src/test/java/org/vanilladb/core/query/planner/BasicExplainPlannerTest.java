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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.parse.CreateViewData;
import org.vanilladb.core.query.parse.DeleteData;
import org.vanilladb.core.query.parse.InsertData;
import org.vanilladb.core.query.parse.ModifyData;
import org.vanilladb.core.query.parse.Parser;
import org.vanilladb.core.query.parse.ExplainData;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.tx.Transaction;

public class BasicExplainPlannerTest {
	private static Logger logger = Logger
			.getLogger(BasicExplainPlannerTest.class.getName());
	private Transaction tx;
	
	@BeforeClass
	public static void init() {
		ServerInit.init(BasicExplainPlannerTest.class);
		ServerInit.loadTestbed();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN EXPLAIN PLANNER TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH EXPLAIN PLANNER TEST");
	}
	
	
	@Before
	public void createTx() {
		tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
	}
	
	@After
	public void finishTx() {
		tx.commit();
		tx = null;
	}

	@Test
	public void testExplain() {
		String qry = "explain select sid, sname, majorid from student, dept "
				+ "where majorid=did and dname='dept0'";
		Parser psr = new Parser(qry);
		ExplainData qd = psr.explainCommand();
		Plan p = new BasicExplainPlanner().explainQuery(qd, tx);
		Schema sch = p.schema();
		assertTrue("*****PlannerTest: bad basic plan schema", sch.fields()
				.size() == 3
				&& sch.hasField("sid")
				&& sch.hasField("sname")
				&& sch.hasField("majorid"));
        logger.info("Explain Test: " + p.toString());
		// Scan s = p.open();
		// s.beforeFirst();
		// while (s.next())
		// 	assertEquals("*****PlannerTest: bad basic plan selection",
		// 			(Integer) 0, (Integer) s.getVal("majorid").asJavaVal());
		// s.close();
	}
}
