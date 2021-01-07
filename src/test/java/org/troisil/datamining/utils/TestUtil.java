package org.troisil.datamining.utils;

import java.util.Arrays;
import java.util.List;

public class TestUtil {
    public static List<HBaseRow> buildTestData() {
        return Arrays.asList(
                HBaseRow.builder().key("k1").operateur("orange").region("haute-vienne").nb_sites_2g(1).nb_sites_3g(2).nb_sites_4g(1).build(),
                HBaseRow.builder().key("k2").operateur("bouygues").region("haute-vienne").nb_sites_2g(1).nb_sites_3g(2).nb_sites_4g(1).build()
        );
    }
}
