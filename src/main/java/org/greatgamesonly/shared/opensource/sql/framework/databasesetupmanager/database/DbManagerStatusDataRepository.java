package org.greatgamesonly.shared.opensource.sql.framework.databasesetupmanager.database;

import org.greatgamesonly.shared.opensource.sql.framework.databasesetupmanager.exceptions.DbManagerException;

import java.util.Map;

@Repository(dbEntityClass = DbManagerStatusData.class)
class DbManagerStatusDataRepository extends BaseRepository<DbManagerStatusData> {

    public DbManagerStatusDataRepository() {}

    @Override
    public Map<String, String> getDbConnectionDetails() throws DbManagerException {
        return Map.of("DatabaseUrl",  DbManagerUtils.getDatabaseUrl(),
                "User", DbManagerUtils.getDatabaseUsername(),
                "Password", DbManagerUtils.getDatabasePassword());
    }
}
