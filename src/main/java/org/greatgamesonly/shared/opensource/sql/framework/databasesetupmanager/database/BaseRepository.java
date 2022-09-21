package org.greatgamesonly.shared.opensource.sql.framework.databasesetupmanager.database;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.greatgamesonly.opensource.utils.reflectionutils.ReflectionUtils;
import org.greatgamesonly.shared.opensource.sql.framework.databasesetupmanager.exceptions.DbManagerException;
import org.greatgamesonly.shared.opensource.sql.framework.databasesetupmanager.exceptions.errors.DbManagerError;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.greatgamesonly.shared.opensource.sql.framework.databasesetupmanager.database.DbUtils.*;

abstract class BaseRepository<E extends BaseEntity> {

    private static Connection connection;
    private Class<E> dbEntityClass;

    protected BaseRepository() {}

    private Class<E> getDbEntityClass() {
        if(dbEntityClass == null) {
            dbEntityClass = (Class<E>) this.getClass().getAnnotation(Repository.class).dbEntityClass();
        }
        return dbEntityClass;
    }

    protected abstract Map<String, String> getDbConnectionDetails() throws DbManagerException;

    protected E getById(Long id) throws DbManagerException {
        List<E> entitiesRetrieved = executeGetQuery("SELECT * FROM " + getDbEntityClass().getAnnotation(Entity.class).tableName() + " WHERE " + getPrimaryKeyDbColumnName(getDbEntityClass()) + " = " + id);
        return (entitiesRetrieved != null && !entitiesRetrieved.isEmpty()) ? entitiesRetrieved.get(0) : null;
    }

    protected List<E> getAll() throws DbManagerException {
        return executeGetQuery("SELECT * FROM " + getDbEntityClass().getAnnotation(Entity.class).tableName());
    }

    protected List<E> getAllByMinAndMaxAndColumnName(Object minId, Object maxId, String columnName, String additionalWhereQuery) throws DbManagerException {
        return executeGetQuery("SELECT * FROM " + getDbEntityClass().getAnnotation(Entity.class).tableName() +
                " WHERE " + columnName + " >= " + returnPreparedValueForQuery(minId) +
                " AND " + columnName + " <= " + returnPreparedValueForQuery(maxId) +
                ((additionalWhereQuery != null && !additionalWhereQuery.isBlank()) ? " AND " + additionalWhereQuery : ""));
    }

    public void deleteById(Long id) throws DbManagerException {
        executeDeleteQuery("DELETE FROM " + getDbEntityClass().getAnnotation(Entity.class).tableName() + " WHERE " + getPrimaryKeyDbColumnName(getDbEntityClass()) + " = " + id);
    }

    protected E getByColumnName(String columnName, Object columnValue) throws DbManagerException {
        List<E> entitiesRetrieved = executeGetQuery("SELECT * FROM " +
                getDbEntityClass().getAnnotation(Entity.class).tableName() + " WHERE " + columnName + " = " +
                returnPreparedValueForQuery(columnValue));
        return (entitiesRetrieved != null && !entitiesRetrieved.isEmpty()) ? entitiesRetrieved.get(0) : null;
    }
    protected E getByColumnNameOrderByColumn(String columnName, Object columnValue, String orderByColumn, OrderBy descOrAsc) throws DbManagerException {
        List<E> entitiesRetrieved = executeGetQuery("SELECT * FROM " +
                getDbEntityClass().getAnnotation(Entity.class).tableName() + " WHERE " + columnName + " = " +
                returnPreparedValueForQuery(columnValue) +
                descOrAsc.getQueryEquivalent(orderByColumn));
        return (entitiesRetrieved != null && !entitiesRetrieved.isEmpty()) ? entitiesRetrieved.get(0) : null;
    }

    protected Long countByColumn(String columnName, Object columnKey) throws DbManagerException {
        try {
            long countTotal;
            ResultSet resultSet = executeQueryRaw("SELECT COUNT(*) AS total FROM " +
                    getDbEntityClass().getAnnotation(Entity.class).tableName() + " WHERE " + columnName + " = " +
                    returnPreparedValueForQuery(columnKey) + ";");
            resultSet.next();
            countTotal = resultSet.getLong("total");
            resultSet.close();
            return countTotal;
        } catch (SQLException e) {
            throw new DbManagerException(DbManagerError.REPOSITORY_COUNT_BY_FIELD__ERROR,e);
        }
    }

    protected Long countByColumns(String columnName, Object columnKey, String columnName2, Object columnKey2) throws DbManagerException {
        try {
            long countTotal;
            ResultSet resultSet = executeQueryRaw("SELECT COUNT(*) AS total FROM " +
                    getDbEntityClass().getAnnotation(Entity.class).tableName() + " WHERE " + columnName + " = " +
                    returnPreparedValueForQuery(columnKey) +
                    " AND " + columnName2 + " = " + returnPreparedValueForQuery(columnKey2) + ";");
            resultSet.next();
            countTotal = resultSet.getLong("total");
            resultSet.close();
            return countTotal;
        } catch (SQLException e) {
            throw new DbManagerException(DbManagerError.REPOSITORY_COUNT_BY_FIELD__ERROR,e);
        }
    }

    protected E insertOrUpdate(E entity) throws DbManagerException {
        E existingEntity = entity.getId() != null ? getById(entity.getId()) : null;
        if(existingEntity == null) {
            existingEntity = insertEntities(entity).get(0);
        } else {
            List<DbEntityColumnToFieldToGetter> dbEntityColumnToFieldToGetters;
            try {
                dbEntityColumnToFieldToGetters = getDbEntityColumnToFieldToGetters(getDbEntityClass());
            } catch (IntrospectionException e) {
                throw new DbManagerException(DbManagerError.REPOSITORY_UPDATE_ENTITY_WITH_ENTITY__ERROR,e);
            }
            E finalExistingEntity = existingEntity;

            for(DbEntityColumnToFieldToGetter dbEntityColumnToFieldToGetter : dbEntityColumnToFieldToGetters) {
                if(dbEntityColumnToFieldToGetter.canBeUpdatedInDb() && !dbEntityColumnToFieldToGetter.isPrimaryKey()) {
                    try {
                        ReflectionUtils.callReflectionMethod(
                                finalExistingEntity,
                                dbEntityColumnToFieldToGetter.getSetterMethodName(),
                                new Object[]{ReflectionUtils.callReflectionMethod(entity, dbEntityColumnToFieldToGetter.getGetterMethodName())},
                                dbEntityColumnToFieldToGetter.getMethodParamTypes()
                        );
                    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                        throw new DbManagerException(DbManagerError.REPOSITORY_CALL_REFLECTION_METHOD__ERROR, e);
                    }
                }
            }
            existingEntity = updateEntities(finalExistingEntity).get(0);
        }
        return existingEntity;
    }

    protected List<E> executeGetQuery(String queryToRun, Object... queryParameters) throws DbManagerException {
        return executeQuery(queryToRun, QueryType.GET, queryParameters);
    }

    protected List<E> executeInsertQuery(String queryToRun, Object... queryParameters) throws DbManagerException {
        return executeQuery(queryToRun, QueryType.INSERT, queryParameters);
    }

    protected List<E> executeUpdateQuery(String queryToRun, Object... queryParameters) throws DbManagerException {
        return executeQuery(queryToRun, QueryType.INSERT, queryParameters);
    }

    protected void executeDeleteQuery(String queryToRun, Object... queryParameters) throws DbManagerException {
        executeQuery(queryToRun, QueryType.DELETE, queryParameters);
    }

    private List<E> executeQuery(String queryToRun, QueryType queryType, Object... queryParameters) throws DbManagerException {
        List<E> entityList = new ArrayList<>();
        try {
            if(queryType.equals(QueryType.INSERT) || queryType.equals(QueryType.UPDATE)) {
                if(queryType.equals(QueryType.INSERT)) {
                    entityList = getRunner().insert(getConnection(), queryToRun, getQueryResultHandler(), queryParameters);
                } else {
                    entityList = getRunner().execute(getConnection(), queryToRun, getQueryResultHandler()).stream().flatMap(List::stream).collect(Collectors.toList());
                }
            } else if(queryType.equals(QueryType.DELETE)) {
                getRunner().execute(getConnection(), queryToRun, getQueryResultHandler());
            } else if(queryType.equals(QueryType.GET)) {
                entityList = getRunner().query(getConnection(), queryToRun, getQueryResultHandler(), queryParameters);
            }
        } catch (SQLException e) {
            if(e.getSQLState().startsWith("23505")) {
                throw new DbManagerException(DbManagerError.REPOSITORY_INSERT_CONSTRAINT_VIOLATION_ERROR, e.getMessage(), e);
            }
            throw new DbManagerException(DbManagerError.REPOSITORY_GET__ERROR,  String.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage()), e);
        } catch (DbManagerException e) {
            throw e;
        } catch (Exception e) {
            throw new DbManagerException(DbManagerError.REPOSITORY_GET__ERROR, e.getMessage() + " non sql error", e);
        } finally {
            try {
                DbUtils.close(getConnection());
            } catch (SQLException e) {
                throw new DbManagerException(DbManagerError.REPOSITORY_GET__ERROR, e);
            }
        }
        return entityList;
    }

    protected ResultSet executeQueryRaw(String queryToRun) throws DbManagerException {
        ResultSet entityList = null;
        try {
            CallableStatement callStatement = getConnection().prepareCall(queryToRun);
            if(queryToRun.startsWith("SELECT")) {
                entityList = callStatement.executeQuery();
            } else {
                callStatement.executeUpdate();
            }
        } catch (SQLException e) {
            System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
            throw new DbManagerException(DbManagerError.REPOSITORY_GET__ERROR,  String.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage()), e);
        } catch (Exception e) {
            throw new DbManagerException(DbManagerError.REPOSITORY_GET__ERROR, e.getMessage() + " non sql error", e);
        } finally {
            try {
                DbUtils.close(getConnection());
            } catch (SQLException e) {
                throw new DbManagerException(DbManagerError.REPOSITORY_GET__ERROR, e);
            }
        }
        return entityList;
    }

    @SafeVarargs
    protected final List<E> insertEntities(E... entitiesToInsert) throws DbManagerException {
        StringBuilder stringBuilder = new StringBuilder();
        try {
            if(entitiesToInsert == null || entitiesToInsert.length <= 0) {
                throw new DbManagerException(DbManagerError.REPOSITORY_INSERT__ERROR, "null or empty entitiesToInsert value was passed");
            }
            List<DbEntityColumnToFieldToGetter> dbEntityColumnToFieldToGetters = getDbEntityColumnToFieldToGetters(getDbEntityClass());

            stringBuilder.append(String.format("INSERT INTO %s (", entitiesToInsert[0].getClass().getAnnotation(Entity.class).tableName()));
            stringBuilder.append(
                dbEntityColumnToFieldToGetters.stream()
                .filter(dbEntityColumnToFieldToGetter ->
                        dbEntityColumnToFieldToGetter.hasSetter() &&
                        !dbEntityColumnToFieldToGetter.isPrimaryKey()
                )
                .map(DbEntityColumnToFieldToGetter::getDbColumnName)
                .collect(Collectors.joining(","))
            );
            stringBuilder.append(") VALUES ");
            for (E entityToInsert : entitiesToInsert) {
                stringBuilder.append("(");

                List<String> toAppendValues = new ArrayList<>();
                for(DbEntityColumnToFieldToGetter dbEntityColumnToFieldToGetter : dbEntityColumnToFieldToGetters) {
                    try {
                        if(dbEntityColumnToFieldToGetter.hasSetter() && !dbEntityColumnToFieldToGetter.isPrimaryKey()) {
                            Object getterValue = ReflectionUtils.callReflectionMethod(entityToInsert, dbEntityColumnToFieldToGetter.getGetterMethodName());
                            toAppendValues.add((getterValue != null) ? returnPreparedValueForQuery(getterValue) : null);
                        }
                    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                        throw new DbManagerException(DbManagerError.REPOSITORY_CALL_REFLECTION_METHOD__ERROR,e);
                    }
                }
                stringBuilder.append(String.join(",",toAppendValues));
                stringBuilder.append(")");
                if (!entityToInsert.equals(entitiesToInsert[entitiesToInsert.length - 1])) {
                    stringBuilder.append(",");
                }
            }
        } catch (Exception e) {
            throw new DbManagerException(DbManagerError.REPOSITORY_PREPARE_INSERT__ERROR, e);
        }
        return executeInsertQuery(stringBuilder.toString());
    }

    protected final List<E> updateEntitiesList(List<? extends BaseEntity> entitiesToUpdate) throws DbManagerException {
        return updateEntities((E[]) entitiesToUpdate.toArray());
    }

    @SafeVarargs
    protected final List<E> updateEntities(E... entitiesToUpdate) throws DbManagerException {
        List<E> result = new ArrayList<>();
        StringBuilder stringBuilder = new StringBuilder();
        try {
            if(entitiesToUpdate == null || entitiesToUpdate.length <= 0) {
                throw new DbManagerException(DbManagerError.REPOSITORY_INSERT__ERROR, "null or empty entitiesToUpdate value was passed");
            }
            List<DbEntityColumnToFieldToGetter> dbEntityColumnToFieldToGetters = getDbEntityColumnToFieldToGetters(getDbEntityClass());
            String primaryKeyColumnName = getPrimaryKeyDbColumnName(dbEntityColumnToFieldToGetters);

            stringBuilder.append(String.format("UPDATE %s SET ", entitiesToUpdate[0].getClass().getAnnotation(Entity.class).tableName()));
            for (BaseEntity entityToUpdate : entitiesToUpdate) {

                List<String> toAppendValues = new ArrayList<>();
                for(DbEntityColumnToFieldToGetter dbEntityColumnToFieldToGetter : dbEntityColumnToFieldToGetters) {
                    try {
                        if(dbEntityColumnToFieldToGetter.hasSetter() &&
                            !dbEntityColumnToFieldToGetter.isPrimaryKey()
                        ) {
                            Object getterValue = ReflectionUtils.callReflectionMethod(entityToUpdate, dbEntityColumnToFieldToGetter.getGetterMethodName());
                            if(getterValue == null && dbEntityColumnToFieldToGetter.isModifyDateAutoSet()) {
                                getterValue = nowDbTimestamp(dbEntityColumnToFieldToGetter.getModifyDateAutoSetTimezone());
                            }
                            toAppendValues.add(dbEntityColumnToFieldToGetter.getDbColumnName() + " = " + ((getterValue != null) ? returnPreparedValueForQuery(getterValue) : null));
                        }
                    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                        throw new DbManagerException(DbManagerError.REPOSITORY_CALL_REFLECTION_METHOD__ERROR,e);
                    }
                }
                stringBuilder.append(String.join(",",toAppendValues));
                if (!entityToUpdate.equals(entitiesToUpdate[entitiesToUpdate.length - 1])) {
                    stringBuilder.append(",");
                } else {
                    stringBuilder.append(String.format(" WHERE %s = %d", primaryKeyColumnName, entityToUpdate.getId()));
                }
                result = executeUpdateQuery(stringBuilder.toString());
            }
        } catch (Exception e) {
            throw new DbManagerException(DbManagerError.REPOSITORY_UPDATE_ENTITY__ERROR, e);
        }
        return result;
    }

    @SafeVarargs
    protected final void deleteEntities(E... entitiesToDelete) throws DbManagerException {
        StringBuilder stringBuilder = new StringBuilder();
        try {
            if(entitiesToDelete == null || entitiesToDelete.length <= 0) {
                throw new DbManagerException(DbManagerError.REPOSITORY_INSERT__ERROR, "null or empty entitiesToDelete value was passed");
            }
            List<DbEntityColumnToFieldToGetter> dbEntityColumnToFieldToGetters = getDbEntityColumnToFieldToGetters(getDbEntityClass());
            String primaryKeyColumnName = getPrimaryKeyDbColumnName(dbEntityColumnToFieldToGetters);

            stringBuilder.append(String.format("DELETE FROM %s WHERE %s IN ( ", entitiesToDelete[0].getClass().getAnnotation(Entity.class).tableName(), primaryKeyColumnName));
            stringBuilder.append(
                Arrays.stream(entitiesToDelete)
                .map(entity -> entity.getId().toString())
                .collect(Collectors.joining(","))
            );
            stringBuilder.append(" );");
            executeDeleteQuery(stringBuilder.toString());
        } catch (Exception e) {
            throw new DbManagerException(DbManagerError.REPOSITORY_DELETE_ENTITY__ERROR, e);
        }
    }

    protected Connection getConnection() throws SQLException, DbManagerException {
        if(connection == null || connection.isClosed()) {
            Map<String, String> dbConnectionDetails = getDbConnectionDetails();

            connection = DriverManager.getConnection(
                    dbConnectionDetails.get("DatabaseUrl"),
                    dbConnectionDetails.get("User"),
                    dbConnectionDetails.get("Password")
            );
            connection.setAutoCommit(true);
        }
        return connection;
    }

    protected BaseBeanListHandler<E> getQueryResultHandler() throws DbManagerException {
        try {
            return new BaseBeanListHandler<>(getDbEntityClass());
        } catch (IntrospectionException | IOException | InterruptedException e) {
            throw new DbManagerException(DbManagerError.REPOSITORY_PREPARE_CLASS__ERROR, e);
        }
    }

    protected QueryRunner getRunner() {
        return new QueryRunner();
    }

    private String getPrimaryKeyDbColumnName(List<DbEntityColumnToFieldToGetter> dbEntityColumnToFieldToGetters) throws DbManagerException {
        return dbEntityColumnToFieldToGetters.stream()
                .filter(DbEntityColumnToFieldToGetter::isPrimaryKey)
                .findFirst().orElseThrow(() -> new DbManagerException(DbManagerError.REPOSITORY_UPDATE_ENTITY__ERROR, "unable to determine primaryKey"))
                .getDbColumnName();
    }

    private String getPrimaryKeyDbColumnName(Class<? extends BaseEntity> dbEntityClass) throws DbManagerException {
        try {
            return getDbEntityColumnToFieldToGetters(dbEntityClass).stream()
                    .filter(DbEntityColumnToFieldToGetter::isPrimaryKey)
                    .findFirst().orElseThrow(() -> new DbManagerException(DbManagerError.REPOSITORY_UPDATE_ENTITY__ERROR, "unable to determine primaryKey"))
                    .getDbColumnName();
        } catch(IntrospectionException e) {
            throw new DbManagerException(DbManagerError.REPOSITORY_RUN_QUERY__ERROR,e.getMessage());
        }
    }

    protected enum QueryType {
        INSERT,
        UPDATE,
        DELETE,
        GET
    }

    protected enum OrderBy {
        DESC("DESC"),
        ASC("ASC");

        private final String queryBase;

        OrderBy(String queryBase) {
            this.queryBase = queryBase;
        }
        private String getQueryEquivalent(String relevantFieldName) {
            switch(this) {
                case DESC:
                case ASC:
                    return " ORDER BY " + relevantFieldName + " " + queryBase;
                default:
                    return queryBase;
            }
        }
    }

}
