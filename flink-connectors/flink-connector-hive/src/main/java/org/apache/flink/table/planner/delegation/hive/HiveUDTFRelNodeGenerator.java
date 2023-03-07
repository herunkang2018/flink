/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.delegation.hive;

import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserQB;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserQBParseInfo;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserRowResolver;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserSqlFunctionConverter;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserTypeConverter;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql2rel.DeduplicateCorrelateVariables;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.buildHiveToCalciteColumnMap;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.getColumnInternalName;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.getCorrelationUse;

/**
 * A generator for generating Hive's UDTF plan, which allows it to contain an udtf node and other
 * expressions.
 */
public class HiveUDTFRelNodeGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(HiveUDTFRelNodeGenerator.class);

    private final FrameworkConfig frameworkConfig;
    private final RelOptCluster cluster;
    private final SqlFunctionConverter funcConverter;
    private final LinkedHashMap<RelNode, HiveParserRowResolver> relToRowResolver;
    private final LinkedHashMap<RelNode, Map<String, Integer>> relToHiveColNameCalcitePosMap;
    private final Prepare.CatalogReader catalogReader;
    private final PlannerContext plannerContext;

    public HiveUDTFRelNodeGenerator(
            FrameworkConfig frameworkConfig,
            RelOptCluster cluster,
            SqlFunctionConverter funcConverter,
            LinkedHashMap<RelNode, HiveParserRowResolver> relToRowResolver,
            LinkedHashMap<RelNode, Map<String, Integer>> relToHiveColNameCalcitePosMap,
            Prepare.CatalogReader catalogReader,
            PlannerContext plannerContext) {
        this.frameworkConfig = frameworkConfig;
        this.cluster = cluster;
        this.funcConverter = funcConverter;
        this.relToRowResolver = relToRowResolver;
        this.relToHiveColNameCalcitePosMap = relToHiveColNameCalcitePosMap;
        this.catalogReader = catalogReader;
        this.plannerContext = plannerContext;
    }

    public RelNode genUDTFPlan(
            SqlOperator sqlOperator,
            String genericUDTFName,
            String outputTableAlias,
            List<String> colAliases,
            HiveParserQB qb,
            List<RexNode> operands,
            List<ColumnInfo> opColInfos,
            RelNode input,
            boolean inSelect,
            boolean isOuter,
            int udtfPos,
            List<RexNode> extraProNodes,
            List<ColumnInfo> extraColInfos)
            throws SemanticException {
        Preconditions.checkState(!isOuter || !inSelect, "OUTER is not supported for SELECT UDTF");
        // No GROUP BY / DISTRIBUTE BY / SORT BY / CLUSTER BY
        HiveParserQBParseInfo qbp = qb.getParseInfo();
        if (inSelect && !qbp.getDestToGroupBy().isEmpty()) {
            throw new SemanticException(ErrorMsg.UDTF_NO_GROUP_BY.getMsg());
        }
        if (inSelect && !qbp.getDestToDistributeBy().isEmpty()) {
            throw new SemanticException(ErrorMsg.UDTF_NO_DISTRIBUTE_BY.getMsg());
        }
        if (inSelect && !qbp.getDestToSortBy().isEmpty()) {
            throw new SemanticException(ErrorMsg.UDTF_NO_SORT_BY.getMsg());
        }
        if (inSelect && !qbp.getDestToClusterBy().isEmpty()) {
            throw new SemanticException(ErrorMsg.UDTF_NO_CLUSTER_BY.getMsg());
        }
        if (inSelect && !qbp.getAliasToLateralViews().isEmpty()) {
            throw new SemanticException(ErrorMsg.UDTF_LATERAL_VIEW.getMsg());
        }

        LOG.debug("Table alias: " + outputTableAlias + " Col aliases: " + colAliases);

        // Create the object inspector for the input columns and initialize the UDTF
        RelDataType relDataType =
                HiveParserUtils.inferReturnTypeForOperands(
                        sqlOperator, operands, cluster.getTypeFactory());
        DataType dataType = HiveParserUtils.toDataType(relDataType);
        StructObjectInspector outputOI =
                (StructObjectInspector)
                        HiveInspectors.getObjectInspector(
                                HiveTypeUtil.toHiveTypeInfo(dataType, false));

        // make up a table alias if it's not present, so that we can properly generate a combined RR
        // this should only happen for select udtf
        if (outputTableAlias == null) {
            Preconditions.checkState(inSelect, "Table alias not specified for lateral view");
            String prefix = "select_" + genericUDTFName + "_alias_";
            int i = 0;
            while (qb.getAliases().contains(prefix + i)) {
                i++;
            }
            outputTableAlias = prefix + i;
        }
        if (colAliases.isEmpty()) {
            // user did not specify alias names, infer names from outputOI
            for (StructField field : outputOI.getAllStructFieldRefs()) {
                colAliases.add(field.getFieldName());
            }
        }
        // Make sure that the number of column aliases in the AS clause matches the number of
        // columns output by the UDTF
        int numOutputCols = outputOI.getAllStructFieldRefs().size();
        int numSuppliedAliases = colAliases.size();
        if (numOutputCols != numSuppliedAliases) {
            throw new SemanticException(
                    ErrorMsg.UDTF_ALIAS_MISMATCH.getMsg(
                            "expected "
                                    + numOutputCols
                                    + " aliases "
                                    + "but got "
                                    + numSuppliedAliases));
        }

        // Generate the output column info's / row resolver using internal names.
        ArrayList<ColumnInfo> udtfOutputCols = new ArrayList<>();

        Iterator<String> colAliasesIter = colAliases.iterator();
        for (StructField sf : outputOI.getAllStructFieldRefs()) {
            String colAlias = colAliasesIter.next();
            assert (colAlias != null);

            // Since the UDTF operator feeds into a LVJ operator that will rename all the internal
            // names,
            // we can just use field name from the UDTF's OI as the internal name
            ColumnInfo col =
                    new ColumnInfo(
                            sf.getFieldName(),
                            TypeInfoUtils.getTypeInfoFromObjectInspector(
                                    sf.getFieldObjectInspector()),
                            outputTableAlias,
                            false);
            udtfOutputCols.add(col);
        }

        // Create the row resolver for the table function scan
        HiveParserRowResolver udtfOutRR = new HiveParserRowResolver();
        for (int i = 0; i < udtfOutputCols.size(); i++) {
            udtfOutRR.put(outputTableAlias, colAliases.get(i), udtfOutputCols.get(i));
        }

        // Build row type from field <type, name>
        RelDataType retType = HiveParserTypeConverter.getType(cluster, udtfOutRR, null);

        List<RelDataType> argTypes = new ArrayList<>();

        RelDataTypeFactory dtFactory = cluster.getRexBuilder().getTypeFactory();
        for (ColumnInfo ci : opColInfos) {
            argTypes.add(HiveParserUtils.toRelDataType(ci.getType(), dtFactory));
        }

        SqlOperator calciteOp =
                HiveParserSqlFunctionConverter.getCalciteFn(
                        genericUDTFName, argTypes, retType, false, funcConverter);

        RexNode rexNode = cluster.getRexBuilder().makeCall(calciteOp, operands);

        // convert the rex call
        TableFunctionConverter udtfConverter =
                new TableFunctionConverter(
                        cluster,
                        input,
                        frameworkConfig.getOperatorTable(),
                        catalogReader.nameMatcher());
        RexCall convertedCall = (RexCall) rexNode.accept(udtfConverter);

        SqlOperator convertedOperator = convertedCall.getOperator();
        Preconditions.checkState(
                convertedOperator instanceof BridgingSqlFunction,
                "Expect operator to be "
                        + BridgingSqlFunction.class.getSimpleName()
                        + ", actually got "
                        + convertedOperator.getClass().getSimpleName());

        // TODO: how to decide this?
        Type elementType = Object[].class;
        // create LogicalTableFunctionScan
        RelNode tableFunctionScan =
                LogicalTableFunctionScan.create(
                        input.getCluster(),
                        Collections.emptyList(),
                        convertedCall,
                        elementType,
                        retType,
                        null);

        // remember the table alias for the UDTF so that we can reference the cols later
        qb.addAlias(outputTableAlias);

        RelNode correlRel;
        RexBuilder rexBuilder = cluster.getRexBuilder();
        // find correlation in the converted call
        Pair<List<CorrelationId>, ImmutableBitSet> correlUse = getCorrelationUse(convertedCall);
        // create correlate node
        if (correlUse == null) {
            correlRel =
                    plannerContext
                            .createRelBuilder()
                            .push(input)
                            .push(tableFunctionScan)
                            .join(
                                    isOuter ? JoinRelType.LEFT : JoinRelType.INNER,
                                    rexBuilder.makeLiteral(true))
                            .build();
        } else {
            if (correlUse.left.size() > 1) {
                tableFunctionScan =
                        DeduplicateCorrelateVariables.go(
                                rexBuilder,
                                correlUse.left.get(0),
                                Util.skip(correlUse.left),
                                tableFunctionScan);
            }
            correlRel =
                    LogicalCorrelate.create(
                            input,
                            tableFunctionScan,
                            correlUse.left.get(0),
                            correlUse.right,
                            isOuter ? JoinRelType.LEFT : JoinRelType.INNER);
        }

        // Add new rel & its RR to the maps
        relToHiveColNameCalcitePosMap.put(
                tableFunctionScan, buildHiveToCalciteColumnMap(udtfOutRR));
        relToRowResolver.put(tableFunctionScan, udtfOutRR);

        HiveParserRowResolver correlRR =
                HiveParserRowResolver.getCombinedRR(
                        relToRowResolver.get(input), relToRowResolver.get(tableFunctionScan));
        relToHiveColNameCalcitePosMap.put(correlRel, buildHiveToCalciteColumnMap(correlRR));
        relToRowResolver.put(correlRel, correlRR);

        if (!inSelect) {
            return correlRel;
        }

        // create project node that will include other rexnodes and udtf
        List<RexNode> projects = new ArrayList<>();
        HiveParserRowResolver projectRR = new HiveParserRowResolver();
        int j = 0;

        for (int i = 0; i < udtfPos; i++) {
            ColumnInfo columnInfo = extraColInfos.get(i);
            String colAlias = columnInfo.getAlias();
            ColumnInfo colInfo =
                    new ColumnInfo(
                            getColumnInternalName(j++),
                            columnInfo.getObjectInspector(),
                            null,
                            false);
            projectRR.put(null, colAlias, colInfo);
            projects.add(extraProNodes.get(i));
        }

        for (int i = input.getRowType().getFieldCount();
                i < correlRel.getRowType().getFieldCount();
                i++) {
            projects.add(cluster.getRexBuilder().makeInputRef(correlRel, i));
            ColumnInfo inputColInfo = correlRR.getRowSchema().getSignature().get(i);
            String colAlias = inputColInfo.getAlias();
            ColumnInfo colInfo =
                    new ColumnInfo(
                            getColumnInternalName(j++),
                            inputColInfo.getObjectInspector(),
                            null,
                            false);
            projectRR.put(null, colAlias, colInfo);
        }

        for (int i = udtfPos; i < extraProNodes.size(); i++) {
            ColumnInfo columnInfo = extraColInfos.get(i);
            String colAlias = columnInfo.getAlias();
            ColumnInfo colInfo =
                    new ColumnInfo(
                            getColumnInternalName(j++),
                            columnInfo.getObjectInspector(),
                            null,
                            false);
            projectRR.put(null, colAlias, colInfo);
            projects.add(extraProNodes.get(i));
        }
        RelDataType finalRelDataType = HiveParserTypeConverter.getType(cluster, projectRR, null);
        RelNode projectNode =
                LogicalProject.create(
                        correlRel, Collections.emptyList(), projects, finalRelDataType);
        relToHiveColNameCalcitePosMap.put(projectNode, buildHiveToCalciteColumnMap(projectRR));
        relToRowResolver.put(projectNode, projectRR);
        return projectNode;
    }
}
