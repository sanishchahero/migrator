package com.migrator.ns.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MigrationRequest {
    private String sourceTable;
    private String sinkTable;

    //For joins
    private String joinTable;
    private String sourceTableCommonColumn;
    private String joinTableCommonColumn;
}