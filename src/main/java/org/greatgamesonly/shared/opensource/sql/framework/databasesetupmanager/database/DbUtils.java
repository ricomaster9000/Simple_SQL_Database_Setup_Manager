package org.greatgamesonly.shared.opensource.sql.framework.databasesetupmanager.database;


import java.beans.IntrospectionException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

class DbUtils {
    protected static final Map<String, List<DbEntityColumnToFieldToGetter>> inMemoryDbEntityColumnToFieldToGetters = new HashMap<>();

    protected static List<DbEntityColumnToFieldToGetter> getDbEntityColumnToFieldToGetters(Class<?> entityClass) throws IntrospectionException {
        if(
            inMemoryDbEntityColumnToFieldToGetters.get(entityClass.getName()) == null ||
            inMemoryDbEntityColumnToFieldToGetters.get(entityClass.getName()).isEmpty()
        ) {
            boolean getSuperClassGettersAndSettersAlso = (entityClass.getSuperclass() != null &&
                    !entityClass.getSuperclass().equals(BaseEntity.class) &&
                    entityClass.getSuperclass().getSuperclass() != null &&
                    entityClass.getSuperclass().getSuperclass().equals(BaseEntity.class));

            inMemoryDbEntityColumnToFieldToGetters.put(entityClass.getName(), new ArrayList<>());
            Field[] fields = ReflectionUtilsImport.getClassFields(entityClass, false, List.of(DBIgnore.class));
            Set<String> getters = ReflectionUtilsImport.getGettersForBaseValueTypes(entityClass.getSuperclass(), true, true);
            Set<String> setters = ReflectionUtilsImport.getSettersForBaseValueTypes(entityClass.getSuperclass(), true, true);

            if(getSuperClassGettersAndSettersAlso) {
                fields = ReflectionUtilsImport.concatenate(fields, ReflectionUtilsImport.getClassFields(entityClass.getSuperclass(),false,List.of(DBIgnore.class)));
                getters.addAll(ReflectionUtilsImport.getGettersForBaseValueTypes(entityClass.getSuperclass(), true, true));
                setters.addAll(ReflectionUtilsImport.getSettersForBaseValueTypes(entityClass.getSuperclass(), true, true));
            }

            for (Field field : fields) {
                DbEntityColumnToFieldToGetter dbEntityColumnToFieldToGetter = new DbEntityColumnToFieldToGetter();
                dbEntityColumnToFieldToGetter.setClassFieldName(field.getName());
                dbEntityColumnToFieldToGetter.setMethodParamTypes(field.getType());

                if(field.isAnnotationPresent(ModifyDateAutoSet.class)) {
                    dbEntityColumnToFieldToGetter.setModifyDateAutoSet(true);
                    dbEntityColumnToFieldToGetter.setModifyDateAutoSetTimezone(field.getAnnotation(ModifyDateAutoSet.class).timezone());
                }
                if(field.isAnnotationPresent(DoNotUpdateInDb.class)) {
                    dbEntityColumnToFieldToGetter.setCanBeUpdatedInDb(false);
                }
                dbEntityColumnToFieldToGetter.setDbColumnName(field.getAnnotation(ColumnName.class).value());

                if(!field.isAnnotationPresent(ColumnName.class) &&
                   (dbEntityColumnToFieldToGetter.getDbColumnName() == null || dbEntityColumnToFieldToGetter.getDbColumnName().isBlank())) {
                    throw new IntrospectionException("annotation not set for db entity field, please set in code");
                }

                if(setters.contains("set" + ReflectionUtilsImport.capitalizeString(dbEntityColumnToFieldToGetter.getClassFieldName()))) {
                    dbEntityColumnToFieldToGetter.setHasSetter(true);
                    dbEntityColumnToFieldToGetter.setSetterMethodName("set" + ReflectionUtilsImport.capitalizeString(dbEntityColumnToFieldToGetter.getClassFieldName()));
                }
                dbEntityColumnToFieldToGetter.setGetterMethodName(
                    getters.stream()
                        .filter(getter -> getter.equals("get" + ReflectionUtilsImport.capitalizeString(dbEntityColumnToFieldToGetter.getClassFieldName())))
                        .findFirst().orElse(null)
                );
                if(field.isAnnotationPresent(PrimaryKey.class)) {
                    dbEntityColumnToFieldToGetter.setIsPrimaryKey(true);
                    dbEntityColumnToFieldToGetter.setPrimaryKeyName(field.getName());
                }
                inMemoryDbEntityColumnToFieldToGetters.get(entityClass.getName()).add(dbEntityColumnToFieldToGetter);
            }
        }
        return inMemoryDbEntityColumnToFieldToGetters.get(entityClass.getName());
    }

    protected static Map<String, String> getColumnsToFieldsMap(Class<?> entityClass) throws IntrospectionException {
        return getDbEntityColumnToFieldToGetters(entityClass)
                .stream()
                .collect(Collectors.toMap(DbEntityColumnToFieldToGetter::getDbColumnName, DbEntityColumnToFieldToGetter::getClassFieldName));
    }

    protected static String returnPreparedValueForQuery(Object object) {
        if(object instanceof String || object instanceof java.util.Date || object.getClass().isEnum()) {
            return "'"+ object +"'";
        } else {
            return object.toString();
        }
    }

    protected static Calendar nowCal(String timezone) {
        return nowCal(timezone, 0);
    }

    protected static Calendar nowCal(String timezone, int minusHours) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone(timezone));
        calendar.add(Calendar.HOUR, minusHours*-1);
        return calendar;
    }

    protected static java.sql.Timestamp nowDbTimestamp() {
        return new java.sql.Timestamp(nowCal("UTC").getTimeInMillis());
    }

    protected static java.sql.Timestamp nowDbTimestamp(int minusHours) {
        return new java.sql.Timestamp(nowCal("UTC", minusHours).getTimeInMillis());
    }

    protected static java.sql.Timestamp nowDbTimestamp(String timezone) {
        return new java.sql.Timestamp(nowCal(timezone).getTimeInMillis());
    }

}
