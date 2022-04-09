package org.vanilladb.core.query.algebra;

import java.util.ArrayList;
import java.util.List;

public class ExplainTree {
    private final List<ExplainTree> children;
    private final String planType;
    private final String details;
    private final Integer blks;
    private final Integer recs;

    public ExplainTree(String planType, String details, Integer blks, Integer recs) {
        this.children = new ArrayList<>();
        this.planType = planType;
        this.details = details;
        this.blks = blks;
        this.recs = recs;
    }

    public void addChild(ExplainTree child) {
        this.children.add(child);
    }

    public List<ExplainTree> getChildren() { return this.children; }

    public String getPlanType() { return this.planType; }

    public String getDetails() { return this.details; }

    public Integer getBlocksAccessed() { return this.blks; }

    public Integer getOutputRecords() { return this.recs; }

}
