package org.apache.hadoop.hive.ql.hooks;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.SetUtils;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.optimizer.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LineageLogger implements ExecuteWithHookContext {
    private static final Logger LOG = LoggerFactory.getLogger(LineageLogger.class);

    private static final HashSet<String> OPERATION_NAMES = new HashSet<>();

    private static final String FORMAT_VERSION = "1.0";

    static {
        OPERATION_NAMES.add(HiveOperation.QUERY.getOperationName());
        OPERATION_NAMES.add(HiveOperation.CREATETABLE_AS_SELECT.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERVIEW_AS.getOperationName());
        OPERATION_NAMES.add(HiveOperation.CREATEVIEW.getOperationName());
    }

    private static void log(String error) {
        SessionState.LogHelper console = SessionState.getConsole();
        if (console != null) {
            console.printError(error);
        }
    }

    @VisibleForTesting
    public static List<Edge> getEdges(QueryPlan plan, LineageCtx.Index index) {
        LinkedHashMap<String, ObjectPair<SelectOperator, org.apache.hadoop.hive.ql.metadata.Table>> finalSelOps = index.getFinalSelectOps();
        Map<String, Vertex> vertexCache = new LinkedHashMap<>();
        List<Edge> edges = new ArrayList<>();
        for (ObjectPair<SelectOperator, org.apache.hadoop.hive.ql.metadata.Table> pair : finalSelOps.values()) {
            List<FieldSchema> fieldSchemas = plan.getResultSchema().getFieldSchemas();
            SelectOperator finalSelOp = (SelectOperator) pair.getFirst();
            org.apache.hadoop.hive.ql.metadata.Table t = pair.getSecond();
            String destTableName = null;
            List<String> colNames = null;
            if (t != null) {
                destTableName = t.getDbName() + "." + t.getTableName();
                fieldSchemas = t.getCols();
            } else {
                for (WriteEntity output : plan.getOutputs()) {
                    Entity.Type entityType = output.getType();
                    if (entityType == Entity.Type.TABLE || entityType == Entity.Type.PARTITION) {
                        t = output.getTable();
                        destTableName = t.getDbName() + "." + t.getTableName();
                        List<FieldSchema> cols = t.getCols();
                        if (cols != null && !cols.isEmpty())
                            colNames = Utilities.getColumnNamesFromFieldSchema(cols);
                        break;
                    }
                }
            }
            Map<ColumnInfo, LineageInfo.Dependency> colMap = index.getDependencies((Operator) finalSelOp);
            List<LineageInfo.Dependency> dependencies = (colMap != null) ? Lists.newArrayList(colMap.values()) : null;
            int fields = fieldSchemas.size();
            if (t != null && colMap != null && fields < colMap.size()) {
                List<FieldSchema> partitionKeys = t.getPartitionKeys();
                int dynamicKeyCount = colMap.size() - fields;
                int keyOffset = partitionKeys.size() - dynamicKeyCount;
                if (keyOffset >= 0) {
                    fields += dynamicKeyCount;
                    for (int j = 0; j < dynamicKeyCount; j++) {
                        FieldSchema field = partitionKeys.get(keyOffset + j);
                        fieldSchemas.add(field);
                        if (colNames != null) {
                            colNames.add(field.getName());
                        }
                    }
                }
            }
            if (dependencies == null || dependencies.size() != fields) {
                log("Result schema has " + fields + " fields, but we don't get as many dependencies");
                continue;
            }
            Set<Vertex> targets = new LinkedHashSet<>();
            for (int i = 0; i < fields; i++) {
                Vertex target = getOrCreateVertex(vertexCache, getTargetFieldName(i, destTableName, colNames, fieldSchemas), Vertex.Type.COLUMN);
                targets.add(target);
                LineageInfo.Dependency dep = dependencies.get(i);
                addEdge(vertexCache, edges, dep.getBaseCols(), target, dep.getExpr(), Edge.Type.PROJECTION);
            }
            Set<LineageInfo.Predicate> conds = index.getPredicates((Operator) finalSelOp);
            if (conds != null && !conds.isEmpty()) {
                for (LineageInfo.Predicate cond : conds) {
                    addEdge(vertexCache, edges, cond.getBaseCols(), new LinkedHashSet<>(targets), cond.getExpr(), Edge.Type.PREDICATE);
                }
            }
        }
        return edges;
    }

    private static void addEdge(Map<String, Vertex> vertexCache, List<Edge> edges, Set<LineageInfo.BaseColumnInfo> srcCols, Vertex target, String expr, Edge.Type type) {
        Set<Vertex> targets = new LinkedHashSet<>();
        targets.add(target);
        addEdge(vertexCache, edges, srcCols, targets, expr, type);
    }

    private static void addEdge(Map<String, Vertex> vertexCache, List<Edge> edges, Set<LineageInfo.BaseColumnInfo> srcCols, Set<Vertex> targets, String expr, Edge.Type type) {
        Set<Vertex> sources = createSourceVertices(vertexCache, srcCols);
        Edge edge = findSimilarEdgeBySources(edges, sources, expr, type);
        if (edge == null) {
            edges.add(new Edge(sources, targets, expr, type));
        } else {
            edge.targets.addAll(targets);
        }
    }

    private static Set<Vertex> createSourceVertices(Map<String, Vertex> vertexCache, Collection<LineageInfo.BaseColumnInfo> baseCols) {
        Set<Vertex> sources = new LinkedHashSet<>();
        if (baseCols != null && !baseCols.isEmpty())
            for (LineageInfo.BaseColumnInfo col : baseCols) {
                Table table = col.getTabAlias().getTable();
                if (table.isTemporary()) {
                    continue;
                }
                Vertex.Type type = Vertex.Type.TABLE;
                String tableName = table.getDbName() + "." + table.getTableName();
                FieldSchema fieldSchema = col.getColumn();
                String label = tableName;
                if (fieldSchema != null) {
                    type = Vertex.Type.COLUMN;
                    label = tableName + "." + fieldSchema.getName();
                }
                sources.add(getOrCreateVertex(vertexCache, label, type));
            }
        return sources;
    }

    private static Vertex getOrCreateVertex(Map<String, Vertex> vertices, String label, Vertex.Type type) {
        Vertex vertex = vertices.get(label);
        if (vertex == null) {
            vertex = new Vertex(label, type);
            vertices.put(label, vertex);
        }
        return vertex;
    }

    private static Edge findSimilarEdgeBySources(List<Edge> edges, Set<Vertex> sources, String expr, Edge.Type type) {
        for (Edge edge : edges) {
            if (edge.type == type && StringUtils.equals(edge.expr, expr) && SetUtils.isEqualSet(edge.sources, sources)) {
                return edge;
            }
        }
        return null;
    }

    private static String getTargetFieldName(int fieldIndex, String destTableName, List<String> colNames, List<FieldSchema> fieldSchemas) {
        String fieldName = ((FieldSchema) fieldSchemas.get(fieldIndex)).getName();
        String[] parts = fieldName.split("\\.");
        if (destTableName != null) {
            String colName = parts[parts.length - 1];
            if (colNames != null && !colNames.contains(colName)) {
                colName = colNames.get(fieldIndex);
            }
            return destTableName + "." + colName;
        }
        if (parts.length == 2 && parts[0].startsWith("_u")) {
            return parts[1];
        }
        return fieldName;
    }

    @VisibleForTesting
    public static Set<Vertex> getVertices(List<Edge> edges) {
        Set<Vertex> vertices = new LinkedHashSet<>();
        for (Edge edge : edges) {
            vertices.addAll(edge.targets);
        }
        for (Edge edge : edges) {
            vertices.addAll(edge.sources);
        }
        int id = 0;
        for (Vertex vertex : vertices) {
            vertex.id = id++;
        }
        return vertices;
    }

    /***
     * 这里的日志只有字段的 column
     * @param hookContext
     */
    public void run(HookContext hookContext) {
        assert hookContext.getHookType() == HookContext.HookType.POST_EXEC_HOOK;
        QueryPlan plan = hookContext.getQueryPlan();
        LineageCtx.Index index = hookContext.getIndex();
        SessionState ss = SessionState.get();
        if (ss != null && index != null && OPERATION_NAMES.contains(plan.getOperationName()) && !plan.isExplain())
            try {
                StringBuilderWriter out = new StringBuilderWriter(1024);
                JsonWriter writer = new JsonWriter((Writer) out);
                String queryStr = plan.getQueryStr().trim();
                writer.beginObject();
                writer.name("version").value("1.0");
                HiveConf conf = ss.getConf();
                boolean testMode = conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST);
                if (!testMode) {
                    long queryTime = plan.getQueryStartTime().longValue();
                    if (queryTime == 0L) {
                        queryTime = System.currentTimeMillis();
                    }
                    long duration = System.currentTimeMillis() - queryTime;
                    writer.name("user").value(hookContext.getUgi().getUserName());
                    writer.name("timestamp").value(queryTime / 1000L);
                    writer.name("duration").value(duration);
                    writer.name("jobIds");
                    writer.beginArray();
                    List<TaskRunner> tasks = hookContext.getCompleteTaskList();
                    if (tasks != null && !tasks.isEmpty()) {
                        for (TaskRunner task : tasks) {
                            String jobId = task.getTask().getJobID();
                            if (jobId != null) {
                                writer.value(jobId);
                            }
                        }
                    }
                    writer.endArray();
                }
                writer.name("engine").value(HiveConf.getVar((Configuration) conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE));
                writer.name("database").value(ss.getCurrentDatabase());
                writer.name("hash").value(getQueryHash(queryStr));
                writer.name("queryText").value(queryStr);
                List<Edge> edges = getEdges(plan, index);
                Set<Vertex> vertices = getVertices(edges);
                writeEdges(writer, edges, hookContext.getConf());
                writeVertices(writer, vertices);
                writer.endObject();
                writer.close();
                String lineage = out.toString();
                if (testMode) {
                    log(lineage);
                } else {
                    //添加保存在这个位置 将 lineage 保存大你想要保存的位置
                    LOG.info(lineage);
                }
            } catch (Throwable t) {
                log("Failed to log lineage graph, query is not affected\n" + org.apache.hadoop.util.StringUtils.stringifyException(t));
            }
    }

    private void writeEdges(JsonWriter writer, List<Edge> edges, HiveConf conf) throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        writer.name("edges");
        writer.beginArray();
        for (Edge edge : edges) {
            writer.beginObject();
            writer.name("sources");
            writer.beginArray();
            for (Vertex vertex : edge.sources) {
                writer.value(vertex.id);
            }
            writer.endArray();
            writer.name("targets");
            writer.beginArray();
            for (Vertex vertex : edge.targets) {
                writer.value(vertex.id);
            }
            writer.endArray();
            if (edge.expr != null) {
                writer.name("expression").value(HookUtils.redactLogString(conf, edge.expr));
            }
            writer.name("edgeType").value(edge.type.name());
            writer.endObject();
        }
        writer.endArray();
    }

    private void writeVertices(JsonWriter writer, Set<Vertex> vertices) throws IOException {
        writer.name("vertices");
        writer.beginArray();
        for (Vertex vertex : vertices) {
            writer.beginObject();
            writer.name("id").value(vertex.id);
            writer.name("vertexType").value(vertex.type.name());
            writer.name("vertexId").value(vertex.label);
            writer.endObject();
        }
        writer.endArray();
    }

    private String getQueryHash(String queryStr) {
        Hasher hasher = Hashing.md5().newHasher();
        hasher.putString(queryStr);
        return hasher.hash().toString();
    }

    @VisibleForTesting
    public static final class Edge {
        private Set<LineageLogger.Vertex> sources;

        private Set<LineageLogger.Vertex> targets;

        private String expr;

        private Type type;

        Edge(Set<LineageLogger.Vertex> sources, Set<LineageLogger.Vertex> targets, String expr, Type type) {
            this.sources = sources;
            this.targets = targets;
            this.expr = expr;
            this.type = type;
        }

        public enum Type {
            PROJECTION, PREDICATE;
        }
    }

    @VisibleForTesting
    public static final class Vertex {
        private Type type;

        private String label;

        private int id;

        Vertex(String label) {
            this(label, Type.COLUMN);
        }

        Vertex(String label, Type type) {
            this.label = label;
            this.type = type;
        }

        public int hashCode() {
            return this.label.hashCode() + this.type.hashCode() * 3;
        }

        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (!(obj instanceof Vertex))
                return false;
            Vertex vertex = (Vertex) obj;
            return (this.label.equals(vertex.label) && this.type == vertex.type);
        }

        @VisibleForTesting
        public Type getType() {
            return this.type;
        }

        @VisibleForTesting
        public String getLabel() {
            return this.label;
        }

        @VisibleForTesting
        public int getId() {
            return this.id;
        }

        public enum Type {
            COLUMN, TABLE;
        }
    }
}
