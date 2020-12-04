package org.troisil.datamining.functions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.function.Function;
import static org.apache.spark.sql.functions.*;

public class SiteCountPerOperator implements Function<Dataset<Row>, Dataset<Row>> {
    @Override
    public Dataset<Row> apply(Dataset<Row> ds) {
        /**
         * Récupérer le nombre de site 2G, 3G, 4G de chaque opérateur
         *
         * Etapes:
         * 1- Récupérer uniqueme les coloonnes dont on a besoin dans le dataset
         * 2- Regrouper les données par opérateur
         * 3- Compter pour chaque opérateur le nombre d'opérateur 2G, 3G, 44G
         */

        return ds.select(column("nom_op").as("operateur"), column("site_2g"), column("site_3g"), column("site_4g"))
                .groupBy("operateur")
                .agg(
                        sum(when(col("site_2g").equalTo(1), 1)).as("nb_sites_2g"),
                        sum(when(col("site_3g").equalTo(1), 1)).as("nb_sites_3g"),
                        sum(when(col("site_4g").equalTo(1), 1)).as("nb_sites_4g")
                );
    }
}
