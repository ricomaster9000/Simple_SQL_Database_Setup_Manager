package org.greatgamesonly.shared.opensource.sql.framework.databasesetupmanager.database;

import org.greatgamesonly.shared.opensource.sql.framework.databasesetupmanager.exceptions.DbManagerException;
import org.greatgamesonly.shared.opensource.sql.framework.databasesetupmanager.exceptions.errors.DbManagerError;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.greatgamesonly.opensource.utils.resourceutils.ResourceUtils.*;

public class DbManagerUtils {

    protected static String getDatabaseUrl() throws DbManagerException {
        String result = getProperty("datasource.url");
        if(result == null || result.isBlank()) {
            result = getProperty("quarkus.datasource.url");
        }
        if(result == null || result.isBlank()) {
            result = getProperty("DATABASE_URL");
        }
        if(result == null || result.isBlank()) {
            throw new DbManagerException(DbManagerError.UNABLE_TO_GET_DATABASE_CONNECTION_DETAILS);
        }
        /*int lastIndexForwardSlash = result.lastIndexOf("/");
        if(lastIndexForwardSlash > 0 && result.toCharArray()[lastIndexForwardSlash-1] != '/') {
            result = result.substring(0,lastIndexForwardSlash) + "/postgres";
        }*/
        return result;
    }

    protected static String getDatabaseUsername() throws DbManagerException {
        String result = getProperty("datasource.username");
        if(result == null || result.isBlank()) {
            result = getProperty("quarkus.datasource.username");
        }
        if(result == null || result.isBlank()) {
            result = getProperty("DATABASE_USERNAME");
        }
        if(result == null || result.isBlank()) {
            throw new DbManagerException(DbManagerError.UNABLE_TO_GET_DATABASE_CONNECTION_DETAILS);
        }
        return result;
    }

    protected static String getDatabasePassword() throws DbManagerException {
        String result = getProperty("datasource.password");
        if(result == null || result.isBlank()) {
            result = getProperty("quarkus.datasource.password");
        }
        if(result == null || result.isBlank()) {
            result = getProperty("DATABASE_PASSWORD");
        }
        if(result == null || result.isBlank()) {
            throw new DbManagerException(DbManagerError.UNABLE_TO_GET_DATABASE_CONNECTION_DETAILS);
        }
        return result;
    }

    protected static String getSeedFileResourceDirectory() throws DbManagerException {
        String result = getProperty("databasesetupmanager_db_seed_files_directory");
        if(result == null || result.isBlank()) {
            result = "seeds";
        }
        return result;
    }

    protected static String getMigrationFileResourceDirectory() throws DbManagerException {
        String result = getProperty("databasesetupmanager_db_migration_files_directory");
        if(result == null || result.isBlank()) {
            result = "migrations";
        }
        return result;
    }

    public static void runDbManager() throws DbManagerException {
        Logger logger = Logger.getLogger("Simple_SQL_Database_Setup_Manager");
        logger.info("Simple_SQL_Database_Setup_Manager - BEGIN");
        DbManagerStatusDataRepository dbManagerStatusDataRepository = new DbManagerStatusDataRepository();
        DbManagerStatusData dbManagerStatusData = null;

        // check if relevant database exists
        /*String databaseUrl = getDatabaseUrl();
        String databaseName = databaseUrl.substring(databaseUrl.lastIndexOf("/"));

        try {
            long databaseExistsCountRow;
            ResultSet resultSetDbExists = dbManagerStatusDataRepository
                    .executeQueryRaw("SELECT COUNT(*) FROM pg_catalog.pg_database as exists WHERE lower(" + databaseName + ") = lower('" + databaseName + "');");
            resultSetDbExists.next();
            databaseExistsCountRow = resultSetDbExists.getLong("exists");
            resultSetDbExists.close();
            if(databaseExistsCountRow <= 0) {
                // check if db_link extension is installed
                ResultSet resultSetDbExists = dbManagerStatusDataRepository.executeQueryRaw("SELECT COUNT(*) as exists WHERE pg_proc.pronamespace=pg_namespace.oid AND pg_proc.proname LIKE '%dblink%';");
            }
        } catch (Exception e) {

        }*/

        dbManagerStatusDataRepository.executeQueryRaw("CREATE TABLE IF NOT EXISTS \"databasesetupmanager_setup_status_info\" (" +
                "id serial PRIMARY KEY," +
                "seed_files_ran BOOLEAN NOT NULL DEFAULT FALSE," +
                "filename_last_migration_file_successfully_ran VARCHAR(10000) NOT NULL);");

        if(dbManagerStatusDataRepository.countByColumn("id", 1) <= 0) {
            dbManagerStatusData = dbManagerStatusDataRepository.insertOrUpdate(new DbManagerStatusData(1L,false, ""));
        } else {
            dbManagerStatusData = dbManagerStatusDataRepository.getAll().get(0);
        }

        if(!dbManagerStatusData.getSeedFilesRan() && doesDirectoryOrFileExistInDirectory(getSeedFileResourceDirectory())) {
            logger.info("Simple_SQL_Database_Setup_Manager - Processing Seed Files - START");
            try {
                List<String> seedFileNames = new ArrayList<>();
                try {
                    seedFileNames = getAllFileNamesInPath(getSeedFileResourceDirectory(), false);
                } catch (IOException e) {
                    throw new DbManagerException(DbManagerError.UNABLE_TO_FETCH_SEED_FILES, e.getMessage());
                }

                try {
                    HashMap<Long, String> seedNumbersOnlyAndFilenames = new HashMap<>();
                    seedFileNames.forEach(seedFileName -> seedNumbersOnlyAndFilenames.put(Long.parseLong(seedFileName.replaceAll("[^0-9]", "")), seedFileName));
                    List<Long> seedFilenameNumbersOnly = seedNumbersOnlyAndFilenames.keySet().stream().sorted().collect(Collectors.toList());
                    for (Long seedFilenameNumberOnly : seedFilenameNumbersOnly) {
                        String seedFileName = seedNumbersOnlyAndFilenames.get(seedFilenameNumberOnly);
                        String sql = readFileIntoString(seedFileName);
                        logger.info("Simple_SQL_Database_Setup_Manager - Processing Seed File " + seedFileName);
                        dbManagerStatusDataRepository.executeQueryRaw(sql);
                    }
                } catch (IOException e) {
                    throw new DbManagerException(DbManagerError.UNABLE_TO_FETCH_SEED_FILES, e.getMessage());
                }
                dbManagerStatusData.setSeedFilesRan(true);
                dbManagerStatusDataRepository.insertOrUpdate(dbManagerStatusData);
            } catch (Exception e) {
                String[] databaseUrl = getDatabaseUrl().split("/");
                String databaseName = databaseUrl[databaseUrl.length-1];

                //ROLLBACK by deleting all the tables in a schema (including constraints and functions etc.
                dbManagerStatusDataRepository.executeQueryRaw(
                    String.format("DROP SCHEMA public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO postgres; GRANT ALL ON SCHEMA public TO %s;",getDatabaseUsername())
                );
                throw new DbManagerException(DbManagerError.UNABLE_TO_PROCESS_SEED_FILES);
            }
            logger.info("Simple_SQL_Database_Setup_Manager - Processing Seed Files - DONE");
        }

        if(doesDirectoryOrFileExistInDirectory(getMigrationFileResourceDirectory())) {
            List<String> migrationFileNames = new ArrayList<>();
            try {
                migrationFileNames = getAllFileNamesInPath(getMigrationFileResourceDirectory(), false);
            } catch (IOException e) {
                throw new DbManagerException(DbManagerError.UNABLE_TO_FETCH_MIGRATION_FILES, e.getMessage());
            }

            try {
                HashMap<Long, String> migrationNumbersOnlyAndFilenames = new HashMap<>();
                migrationFileNames.forEach(migrationFileName -> migrationNumbersOnlyAndFilenames.put(Long.parseLong(migrationFileName.replaceAll("[^0-9]", "")), migrationFileName));
                List<Long> migrationFilenameNumbersOnly = migrationNumbersOnlyAndFilenames.keySet().stream().sorted().collect(Collectors.toList());

                if (dbManagerStatusData.getFilenameOfLastMigrationFileThatWasRun() != null && !dbManagerStatusData.getFilenameOfLastMigrationFileThatWasRun().isBlank()) {
                    migrationFilenameNumbersOnly =
                            migrationFilenameNumbersOnly.subList(
                                migrationFilenameNumbersOnly.indexOf(Long.parseLong(dbManagerStatusData.getFilenameOfLastMigrationFileThatWasRun().replaceAll("[^0-9]", ""))),
                                migrationFilenameNumbersOnly.size() - 1
                            );
                }
                for (Long migrationFilenameNumberOnly : migrationFilenameNumbersOnly) {
                    String sql = readFileIntoString(migrationNumbersOnlyAndFilenames.get(migrationFilenameNumberOnly));
                    dbManagerStatusDataRepository.executeQueryRaw(sql);
                    dbManagerStatusData.setFilenameOfLastMigrationFileThatWasRun(migrationNumbersOnlyAndFilenames.get(migrationFilenameNumberOnly));
                    dbManagerStatusDataRepository.insertOrUpdate(dbManagerStatusData);
                }
            } catch (IOException e) {
                throw new DbManagerException(DbManagerError.UNABLE_TO_FETCH_MIGRATION_FILES, e.getMessage());
            }
        }
        Logger.getLogger("Simple_SQL_Database_Setup_Manager").info("Simple_SQL_Database_Setup_Manager - END");
    }

}
