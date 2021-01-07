package org.troisil.datamining.utils;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class HBaseRow implements Serializable {
    private String key;
    private String operateur;
    private String region;
    private Integer nb_sites_2g;
    private Integer nb_sites_3g;
    private Integer nb_sites_4g;
}
