package com.migrator.ns.controller;

import com.migrator.ns.dto.MigrationRequest;
import com.migrator.ns.services.MigrationService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/migrate")
@RequiredArgsConstructor
public class MigrationController {

    private final MigrationService migrationService;

    @PostMapping("/ns")
    public String migrate(@RequestBody MigrationRequest migrationRequest) {
        migrationService.proceedMigration(migrationRequest);
        return "Migration completed!";
    }
}