package org.apache.hadoop.hive.ql.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;
import java.util.TreeSet;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class LineageInfo implements NodeProcessor {
    TreeSet<String> inputTableList = new TreeSet();
    TreeSet<String> OutputTableList = new TreeSet();

    public LineageInfo() {
    }

    public TreeSet<String> getInputTableList() {
        return this.inputTableList;
    }

    public TreeSet<String> getOutputTableList() {
        return this.OutputTableList;
    }

    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
        ASTNode pt = (ASTNode)nd;
        switch(pt.getToken().getType()) {
            case 942:
                this.OutputTableList.add(BaseSemanticAnalyzer.getUnescapedName((ASTNode)pt.getChild(0)));
                break;
            case 971:
                ASTNode tabTree = (ASTNode)pt.getChild(0);
                String table_name = tabTree.getChildCount() == 1 ? BaseSemanticAnalyzer.getUnescapedName((ASTNode)tabTree.getChild(0)) : BaseSemanticAnalyzer.getUnescapedName((ASTNode)tabTree.getChild(0)) + "." + tabTree.getChild(1);
                this.inputTableList.add(table_name);
        }

        return null;
    }

    public void getLineageInfo(String query) throws ParseException, SemanticException {
        ASTNode tree;
        for(tree = ParseUtils.parse(query, (Context)null); tree.getToken() == null && tree.getChildCount() > 0; tree = (ASTNode)tree.getChild(0)) {
        }

        this.inputTableList.clear();
        this.OutputTableList.clear();
        Map<Rule, NodeProcessor> rules = new LinkedHashMap();
        Dispatcher disp = new DefaultRuleDispatcher(this, rules, (NodeProcessorCtx)null);
        GraphWalker ogw = new DefaultGraphWalker(disp);
        ArrayList<Node> topNodes = new ArrayList();
        topNodes.add(tree);
        ogw.startWalking(topNodes, (HashMap)null);
    }

    public static void main(String[] args) throws IOException, ParseException, SemanticException {
        String query = "create table ads.t1(c1 string)";
        LineageInfo lep = new LineageInfo();
        lep.getLineageInfo(query);
        Iterator var3 = lep.getInputTableList().iterator();

        String tab;
        while(var3.hasNext()) {
            tab = (String)var3.next();
            System.out.println("InputTable=" + tab);
        }

        var3 = lep.getOutputTableList().iterator();

        while(var3.hasNext()) {
            tab = (String)var3.next();
            System.out.println("OutputTable=" + tab);
        }
    }
}
