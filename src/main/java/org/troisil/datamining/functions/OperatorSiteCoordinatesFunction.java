package org.troisil.datamining.functions;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.execution.RowIterator;

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
         * Rhône Alpes => ["1", "2", ...]
         */
        return ds.filter(operatorNameFilterFunction)
                .groupByKey(groupDataByRegionFunction, Encoders.STRING())
                .flatMapGroups((FlatMapGroupsFunction<String, Row, Row>) (key, values) -> {
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
                }, Encoders.kryo(Row.class));

    }
}
