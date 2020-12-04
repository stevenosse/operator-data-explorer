package org.troisil.datamining.functions;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.execution.RowIterator;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import static org.apache.spark.sql.functions.*;

@Slf4j
@RequiredArgsConstructor
public class OperatorSiteCoordinatesFunction implements Function<Dataset<Row>, Dataset<Row>>, Serializable {
    @NonNull
    final String operatorName;
    @Override
    public Dataset<Row> apply(Dataset<Row> ds) {
        log.info("Filtering data");
        FilterFunction<Row> operatorNameFilterFunction = row -> this.operatorName.equalsIgnoreCase(row.getAs("nom_op"));

        MapFunction<Row, String> groupDataByRegionFunction = (MapFunction<Row, String>) row -> row.getAs("nom_reg");
        /**
         * On veut récuperer l'ensemble des coordonnées géographiques des sites d'un opérateur par région
         *
         * Regrouper par région
         *
         *
         * Résultat attendu:
         * Rhône Alpes => [{x, y}, {x, y}, ...]
         */
        return ds.filter(operatorNameFilterFunction)
                .groupBy("nom_reg")
                .agg(collect_list(concat_ws(", ", col("x_lambert_93"), col("y_lambert_93"))).as("coordinates"));
        /*return ds.filter(operatorNameFilterFunction)
                .groupByKey(groupDataByRegionFunction, Encoders.STRING())
                .reduceGroups(new ReduceFunction<Row>() {
                    @Override
                    public Row call(Row row, Row t1) throws Exception {
                        ArrayList<String> myList = new ArrayList<>();

                        buildCoordinatesList(t1, myList);
                        buildCoordinatesList(row, myList);

                        return RowFactory.create(myList.toArray(new String[]{}));
                    }
                }).toDF();*/
        /*flatMapGroups((FlatMapGroupsFunction<String, Row, Row>) (key, values) -> {
                    ArrayList<HashMap<String, String>> coordinatesList = new ArrayList<>();
                    while(values.hasNext()) {
                        Row r = values.next();
                        HashMap<String, String> coordinates = new HashMap<String, String>();
                        coordinates.put(r.getAs("x_lambert_93"), r.getAs("y_lambert_93"));
                        coordinatesList.add(coordinates);
                    }
                    Row row = RowFactory.create(key, coordinatesList);

                    return new Iterator<Row>() {
                        @Override
                        public boolean hasNext() {
                            return true;
                        }

                        @Override
                        public Row next() {
                            return row;
                        }
                    };
                }, Encoders.bean(Row.class));*/

    }

    private void buildCoordinatesList(Row row, ArrayList<String> myList) {
        if(row != null) {
            if (Arrays.stream(row.schema().fieldNames()).anyMatch(s -> s.equalsIgnoreCase("x_lambert_93") || s.equalsIgnoreCase("y_lambert_93"))) {
                myList.add(String.format("(%s, %s)", row.getAs("x_lambert_93"), row.getAs("y_lambert_93")));
            }
            if(row.schema().fields()[0].dataType().sameType(DataTypes.createArrayType(DataTypes.StringType))){
                myList.addAll(Arrays.asList((String[]) row.get(0)));
            }
        }
    }
}
