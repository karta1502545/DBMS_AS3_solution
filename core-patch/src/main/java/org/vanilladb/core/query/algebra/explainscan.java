package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;
public class explainscan implements Scan {
	private Scan s;
	private String ans;
	private boolean check=true;
	public explainscan(Scan s,String ans) {
		this.s = s;
		this.ans=ans;
	}
	public void beforeFirst() {
		s.beforeFirst();
	}
	public boolean next() {
		if(check) {
			check=false;
			return true;
		}
		else return false;
	}

	@Override
	public void close() {
		s.close();
	}
	public Constant getVal(String fldName) {
		if(fldName.equals("query-plan")) {
			Constant cons=new VarcharConstant(ans);
			return cons;
		}
		else return s.getVal(fldName);
	}
	public boolean hasField(String fldName) {
		return s.hasField(fldName);
	}

}
