package org.troisil.datamining.functions;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.Iterator;
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

        /**
         * On a essayé avec :
         * - mapGroups
         * - JavaPaiRDD (via mapToPair)
         * - agg
         * - groupByKey puis flatMapGroup
         *
         * mais aucune des solutions n'a fonctionné et c'est assez difficile de trouver de la documentation
         * sur Internet
         */

        return ds.filter(operatorNameFilterFunction)
                .select(column("nom_reg"), column("site_4g"))
                .where("site_4g = 1");

    }
}
