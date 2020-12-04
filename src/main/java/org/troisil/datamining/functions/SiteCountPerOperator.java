package org.troisil.datamining.functions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.function.Function;
import static org.apache.spark.sql.functions.*;

public class SiteCountPerOperator implements Function<Dataset<Row>, Dataset<Row>> {
    @Override
    public Dataset<Row> apply(Dataset<Row> ds) {
        /**
         * Récupérer le nombre de site 2G, 3G, 4G de chaque opérateur par region
         *
         * Etapes:
         * 1- Récupérer uniquement les coloonnes dont on a besoin dans le dataset
         * 2- Regrouper les données par opérateur
         * 3- Compter pour chaque opérateur le nombre de sites 2G, 3G, 4G
         */

        return ds
                .filter("site_2g IS NOT null AND site_4g IS NOT null AND site_3g IS NOT null")
                .select(column("nom_op").as("operateur"), column("nom_reg").as("region"), column("site_2g"), column("site_3g"), column("site_4g"))
                .groupBy("operateur", "region")
                .agg(
                        sum(when(col("site_2g").equalTo(1), 1)).as("nb_sites_2g"),
                        sum(when(col("site_3g").equalTo(1), 1)).as("nb_sites_3g"),
                        sum(when(col("site_4g").equalTo(1), 1)).as("nb_sites_4g")
                );
    }
}
