package com.migrator.ns.services;

import com.migrator.ns.dto.MigrationRequest;

public interface MigrationService {
    void proceedMigration(MigrationRequest migrationRequest);
}
