# Simple_SQL_Database_Setup_Manager
Simple&Basic SLQ(Currently only for PSQL) Database Manager library that can manage&run your seed files and migration files for Java 11+

put raw sql into the sql files, under the directories migrations and seeds, under the resource directory

set the following properties inside you properties file:
datasource.url
datasource.username
datasource.password

run DbManagerUtils.runDbManager();

This will firstly run seeds, after seeds are run, seeds will never be run again, and then it will run migrations, it sorts seed and migration files based on numerical value (so if your seed/migration file is named 244343_CREATE_BASE_TABLES.sql etc.) then it will only use the 244343 to determine the order in which to run seed and migration files, it will always be from lowest numerical value to higherst

for migrations it takes note of the last migration file ran, and any new migration files added that is higher than the numerical sorting value of the last migration file that was run will then be run, then then again the last migration file(the filename specificially) run will be used the next time to determine what migration files to run

THERE is currently no solid rollback functionality that exists if seed or migration operations fail, so take note of this, will add in later versions.

add as dependency by using jitpack.io, go to this link: https://jitpack.io/#ricomaster9000/Simple_SQL_Database_Setup_Manager/1.0.7.1

Will upload to Maven later, once I am fully done and have more time
