using Exigo.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;

namespace Exigo.Services.SQLTableCleanUp
{
    public static class SqlTableCleanUpUtil
    {
        /// <summary>
        /// Class used to dynamically build all columns for any given table
        /// which includes the Type, Name and Value in each column
        /// </summary>
        public class DataColumn : Column
        {
            public object Value { get; set; }
            public Type ColumnType { get; set; }
        }
        
        /// <summary>
        /// Class used to dynamically store List of columns found
        /// </summary>
        public class Row
        {
            public List<DataColumn> Columns { get; set; }
        }

        /// <summary>
        /// Object used to store Parent Child combination
        /// </summary>
        public class ParentChild
        {
            public Row Parent { get; set; }
            public Row Child { get; set; }
        }

        //TODO: Ask David how many rows we should pull in at once
        //Max number of rows to compare at any given time
        private static int maxRows = 1000;

        private static bool isReportOnly;

        //Report generated
        public static string SyncReport { get; set; }

        /// <summary>
        /// Method used to find differences between two given tables and remedying those differences
        /// </summary>
        /// <param name="tableMaps">Map defining the From-To column combination</param>
        /// <param name="childConn">Open child connection</param>
        /// <param name="parentConn">Open parent connection</param>
        /// <param name="parentSync">Parent sync schema name</param>
        /// <param name="childSync">Child sync schema name</param>
        /// <returns>Number of cleanups processed</returns>
        public static int CleanUpTables(IEnumerable<TableMap> tableMaps, SqlConnection parentConn, SqlConnection childConn, string parentSync, string childSync, bool reportOnly, out string syncReport)
        {
            var count = 0;
            isReportOnly = reportOnly;
            SyncReport = "";

            foreach (var tableMap in tableMaps)
            {
                //Get parent and child tables from tableMap
                var parentTable = tableMap.FromTable.Schema + "." + tableMap.FromTable.TableName;
                var parentPKs = tableMap.ColumnMaps.Where(cm => cm.FromColumn.IsKey)
                                .ToList()
                                .Select(s => s.FromColumn.Name)
                                .Aggregate((i, j) => i + ", " + j);

                var childTable = tableMap.ToTable.Schema + "." + tableMap.ToTable.TableName;
                var childPKs = tableMap.ColumnMaps.Where(cm => cm.ToColumn.IsKey)
                                .ToList()
                                .Select(s => s.ToColumn.Name)
                                .Aggregate((i, j) => i + ", " + j);

                for (int i = 0; i < 100; i++)
                {
                    //Get All Parent and Child rows to compare
                    var parentRows = GetRows(parentConn, parentTable, parentPKs, parentSync);
                    var childRows = GetRows(childConn, childTable, childPKs, childSync);
                    List<Row> tempRows;

                    //If nothing left to compare, break loop
                    if (parentRows.Count == 0 && childRows.Count == 0) break;

                    //Dictionary where all differences are stored for processing <Guid, ParentChild>
                    //GetDifferences inherently handles when Parent has items, not in the child.
                    //To make sure we know what items are in the Child but not parent, we store them in tempRows and
                    //process it later.
                    var diff = GetDifferences(parentRows, childRows, childConn, childTable, out tempRows);

                    //Because we have a finite amount of rows we pull in, there may be a use case where
                    //a row actually exists but we just didn't pull it in to compare against.
                    //This checks to make sure the row truly doesn't exist before processing.  If it does exist
                    //in the table, remove row from temp storage so we don't try to insert it.
                    if(tempRows.Count > 0)
                        tempRows = RemoveExistingRows(parentConn, parentTable, tempRows);

                    //If tempRows isn't an empty List, we should add to diff as they don't exist 
                    //in parent and need to be processed as an insert
                    foreach (var child in tempRows)
                    {
                        var rowGuid = child.Columns.FirstOrDefault(c => c.Name.Equals("RowGuid"));
                        if (rowGuid != null)
                        {
                            var childRowGuid = (Guid) rowGuid.Value;
                            diff.Add(childRowGuid, new ParentChild {Parent = GetEmptyRow(), Child = child});
                        }
                    }

                    //If we find differences, make neccesary changes
                    if (diff.Count > 0)
                    {
                        SyncTables(tableMap, childConn, parentConn, childTable, parentTable, diff);
                        count += diff.Count;
                    }

                    //Sleep briefly to free up resources before resuming
                    //Thread.Sleep(500);
                }

                //Reset Sequence numbers of Parent and Child for next run
                SetLastSequence(parentConn, parentSync, parentTable, 0);
                SetLastSequence(childConn, childSync, childTable, 0);
            }

            syncReport = SyncReport;

            //Return how many differences we found and fixed
            return count;
        }

        /// <summary>
        /// Method used to dynamically get column information and put them in rows 
        /// </summary>
        /// <param name="conn">Open connection to DB</param>
        /// <param name="table">Table we're getting rows for</param>
        /// <param name="primaryKeys">Primary keys used to order select</param>
        /// <param name="syncSchema">Name of Sync schema to get/set CleanUpSettings</param>
        /// <param name="numberOfRows">Max number of rows to get at any given time</param>
        /// <param name="allowNoLock">Bool to determine whether to use nolock</param>
        /// <returns></returns>
        private static List<Row> GetRows(SqlConnection conn, string table, string primaryKeys, string syncSchema, bool allowNoLock = true)
        {
            //We allow no lock by default. Set to false if we don't want to use it.
            string noLock = "";
            if (allowNoLock && !Sql.IsReadCommittedSnapshotOn(conn))
                noLock = "(nolock)";

            //Value that's populated and returned
            var rows = new List<Row>();

            //Get Sequence numbers
            var startSequence = GetLastSequence(conn, syncSchema, table);
            var endSequence = new long();

            var orderBy = primaryKeys.Trim().Length > 0 ? $"ORDER BY {primaryKeys}" : "";

            //Sql command used to get rows.  It's ordered by primarykeys for predictability
            var sqlString = $@"
                            DECLARE
	                        @START AS BIGINT,
	                        @END AS BIGINT

                            SET @START = {startSequence}
                            SET @END = @START + {maxRows}

                            SELECT TOP {maxRows} *
                            INTO #Temp
                            FROM 
	                            ( SELECT *, 
		                            Row_Number() OVER ({orderBy} DESC) AS [Sequence]
	                              FROM {table} {noLock}
	                            ) AS CompleteSet
                            WHERE [Sequence] > @START AND [Sequence] <= @END

                            SELECT * FROM #Temp
                            SELECT TOP 1 [Sequence] FROM #Temp ORDER BY [Sequence] DESC
                            DROP TABLE #Temp";

            var cmd = new SqlCommand(sqlString, conn);

            //Read data and populate Rows which contain Columns to be returned
            using (var rd = cmd.ExecuteReader())
            {
                do
                {
                    //Get column values and add to list of rows
                    while (rd.Read())
                    {
                        //Set the End Sequence
                        if (rd.GetName(0) == "Sequence")
                            endSequence = rd.GetInt64(0);
                        else
                        {
                            var columns = new List<DataColumn>();
                            var dataTable = rd.GetSchemaTable();
                            if (dataTable != null)
                            {
                                var i = 0;
                                foreach (DataRow row in dataTable.Rows)
                                {
                                    var columnName = row["ColumnName"].ToString();
                                    if (!columnName.Equals("Sequence"))
                                    {
                                        var temp = new DataColumn
                                        {
                                            Name = columnName,
                                            ColumnType = (Type)row["DataType"],
                                            Value = rd.GetValue(i)
                                        };
                                        columns.Add(temp);
                                    }
                                    i++;
                                }

                                rows.Add(new Row { Columns = columns });
                            }
                        }
                    }

                } while (rd.NextResult());
            }

            //If the endSequence changed, set it in DB for future use (now becomes new startSequence)
            if(endSequence != 0) SetLastSequence(conn, syncSchema, table, endSequence);

            return rows;
        }

        /// <summary>
        /// Return a single row with Column data
        /// </summary>
        /// <param name="conn">Connection to table</param>
        /// <param name="rowGuid">Guid which we filter search with</param>
        /// <param name="table">Table we're getting row from</param>
        /// <param name="rowVersion">RowVersion being returned</param>
        /// <returns>A Row with data</returns>
        private static Row GetRow(SqlConnection conn, Guid rowGuid, string table, out long rowVersion)
        {
            rowVersion = 0;
            var tempRow = new Row();
            var sqlString = $@"SELECT TOP 1 * FROM {table} where RowGuid='{rowGuid}'";

            var cmd = new SqlCommand(sqlString, conn);
            using (var rd = cmd.ExecuteReader())
            {
                do
                {
                    //Get column values and add to list of rows
                    while (rd.Read())
                    {
                        var columns = new List<DataColumn>();
                        var dataTable = rd.GetSchemaTable();
                        var i = 0;
                        if (dataTable != null)
                            foreach (DataRow row in dataTable.Rows)
                            {
                                var columnName = row["ColumnName"].ToString();
                                var value = rd.GetValue(i);

                                if (columnName.Equals("RowVersion"))
                                    rowVersion = (long)value;

                                var temp = new DataColumn
                                {
                                    Name = columnName,
                                    ColumnType = (Type)row["DataType"],
                                    Value = value
                                };
                                columns.Add(temp);
                                i++;
                            }

                        tempRow = new Row { Columns = columns };
                    }

                } while (rd.NextResult());
            }

            return tempRow;
        }

        /// <summary>
        /// Method used to figure out if we have differences between two given tables.
        /// It accounts for different RowVersions and missing rows.
        /// </summary>
        /// <param name="parentRows">Rows of data from parent table</param>
        /// <param name="childRows">Rows of data from child table</param>
        /// <param name="emptyRow">Empty row to fill missing table with</param>
        /// <param name="tempRows">Used to determine rows that are in child but not parent.</param>
        /// <param name="childConn">Child connection used to see if rows exist</param>
        /// <param name="childTable">Child table we test to see if rows exist</param>
        /// <returns>Dictionary with columnName as Guid and Value as ParentChild combo.</returns>
        public static Dictionary<Guid, ParentChild> GetDifferences(List<Row> parentRows, List<Row> childRows, SqlConnection childConn, string childTable, out List<Row> tempRows)
        {
            //Dictionary with differences
            var diff = new Dictionary<Guid, ParentChild>();

            //Used to determine which rows remain for child after processing 
            //That means it's not in parent and should be an insert)
            var childCopyRows = new List<Row>(childRows);

            //Check for different RowVersions per Guid (updates) 
            //and for ParentRows that aren't in Child (inserts) and add to diff list
            parentRows.ForEach(parent =>
            {
                var matchFound = false;

                var firstParentRowGuid = parent.Columns.FirstOrDefault(c => c.Name.Equals("RowGuid"));
                var firstParentRowVersion = parent.Columns.FirstOrDefault(c => c.Name.Equals("RowVersion"));
                if (firstParentRowGuid != null && firstParentRowVersion != null)
                {
                    var parentRowGuid = (Guid)firstParentRowGuid.Value;
                    var parentRowVersion = (long)firstParentRowVersion.Value;

                    //Make sure the guid doesn't exist in the child table because of the finite
                    //amount of rows we pull in.
                    var exists = GuidExist(childConn, parentRowGuid, childTable);

                    //Use the rows we did pull in and compare against each other to see if we found a match.
                    childRows.ForEach(child =>
                    {
                        var firstChildRowGuid = child.Columns.FirstOrDefault(c => c.Name.Equals("RowGuid"));
                        var firstChildRowVersion = child.Columns.FirstOrDefault(c => c.Name.Equals("RowVersion"));
                        if (firstChildRowGuid != null && firstChildRowVersion != null)
                        {
                            var childRowGuid = (Guid)firstChildRowGuid.Value;
                            var childRowVersion = (long)firstChildRowVersion.Value;
                            if (parentRowGuid.Equals(childRowGuid))
                            {
                                //If we found a match, remove it from childCopyRows
                                var match = childCopyRows.FirstOrDefault(r => r == child);
                                childCopyRows.Remove(match);
                                matchFound = true;

                                //If we found equal Guids with different RowVersions, add to diff list
                                if (!parentRowVersion.Equals(childRowVersion))
                                    diff.Add(parentRowGuid, new ParentChild { Parent = parent, Child = child });
                            }
                        }
                    });

                    //There may be a use case where because of the number of records we pull in,
                    //we may not have pulled in the record we compare against but it does indeed exist
                    //Because of that, this checks to make sure the row isn't different.  If it is, add
                    //it to diff.
                    if (exists && !matchFound)
                    {
                        long rowVersion;
                        var child = GetRow(childConn, parentRowGuid, childTable, out rowVersion);

                        if (!parentRowVersion.Equals(rowVersion))
                            diff.Add(parentRowGuid, new ParentChild { Parent = parent, Child = child });
                    }
                    else if (!exists && !matchFound)
                    {
                        //If we didn't find a match, that's an insert that needs to happen, add to diff
                        diff.Add(parentRowGuid, new ParentChild { Parent = parent, Child = GetEmptyRow() });
                    }
                }
            });

            tempRows = childCopyRows;
            return diff;
        }

        /// <summary>
        /// Syncs both tables together after verifying which table has greater RowVersion or insert if missing
        /// </summary>
        /// <param name="tableMap">Map defining the From-To column combination</param>
        /// <param name="childConn">Open child connection</param>
        /// <param name="parentConn">Open parent connection</param>
        /// <param name="childTable">Name of Child table</param>
        /// <param name="parentTable">Name of Parent table</param>
        /// <param name="diff">Dictionary with Unique Guid's as columnName and List parent/child rows as value</param>
        private static void SyncTables(TableMap tableMap, SqlConnection childConn, SqlConnection parentConn, string childTable, string parentTable, Dictionary<Guid, ParentChild> diff)
        {
            //For each difference found process it
            foreach (var entry in diff)
            {
                //Get the row versions
                var parentChild = entry.Value;
                var parentRow = parentChild.Parent.Columns.FirstOrDefault(c => c.Name.Equals("RowVersion"));
                var childRow = parentChild.Child.Columns.FirstOrDefault(c => c.Name.Equals("RowVersion"));

                //If it's equal to null, return 0 else return the value.
                //This is a sanity check as it should never be null.
                var parentRowVersion = (long?)parentRow?.Value ?? 0;
                var childRowVersion = (long?)childRow?.Value ?? 0;

                string cmd;
                var sqlCmd = new SqlCommand();

                //If row version is 0, we didn't have a match so an insert needs to happen
                if (parentRowVersion == 0 || childRowVersion == 0)
                {
                    if (parentRowVersion != 0)
                    {
                        //We found a parent that didn't have a matching child.
                        SyncReport += $"GUID: {entry.Key} has an entry in table {parentTable} but not in {childTable}" + " \n";
                        if (!isReportOnly)
                        {
                            var fromColumns = parentChild.Parent.Columns;
                            cmd = InsertSqlCmd(tableMap, fromColumns, childTable, childConn);

                            //Insert into child
                            sqlCmd = new SqlCommand(cmd, childConn);
                        }
                    }
                    else
                    {
                        //We found a child that didn't have a matching parent.
                        SyncReport += $"GUID: {entry.Key} has an entry in table {childTable} but not in {parentTable}" + " \n";
                        if (!isReportOnly)
                        {
                            var fromColumns = parentChild.Child.Columns;
                            cmd = InsertSqlCmd(tableMap, fromColumns, parentTable, parentConn);

                            //Insert into parent
                            sqlCmd = new SqlCommand(cmd, parentConn);
                        }
                    }
                }
                else
                {
                    //Check for different rowVersions between parent and child and update
                    if (parentRowVersion > childRowVersion)
                    {
                        //Parent is more up-to-date, update child
                        SyncReport += $@"GUID: {entry.Key} has a higher RowVersion of {parentRowVersion} in {parentTable} compared to {childRowVersion} in {childTable}" + " \n";
                        if (!isReportOnly)
                        {
                            var fromColumns = parentChild.Parent.Columns;
                            var toColumns = parentChild.Child.Columns;
                            cmd = UpdateSqlCmd(tableMap, fromColumns, toColumns, entry.Key, childTable, parentRowVersion, childConn);

                            //Update the child
                            sqlCmd = new SqlCommand(cmd, childConn);
                        }
                    }
                    else
                    {
                        //Child is more up-to-date, update parent
                        SyncReport += $@"GUID: {entry.Key} has a higher RowVersion of {childRowVersion} in {childTable} compared to {parentRowVersion} in {parentTable}" + " \n";
                        if (!isReportOnly)
                        {
                            var fromColumns = parentChild.Child.Columns;
                            var toColumns = parentChild.Parent.Columns;
                            cmd = UpdateSqlCmd(tableMap, fromColumns, toColumns, entry.Key, parentTable, childRowVersion, parentConn);

                            //Update the parent
                            sqlCmd = new SqlCommand(cmd, parentConn);
                        }
                    }
                }

                if (!isReportOnly)
                {
                    sqlCmd.ExecuteNonQuery();
                    SyncReport += "Differences were remedied \n";
                }
            }
        }

        /// <summary>
        /// Method used to insert into table when row not present in other table
        /// </summary>
        /// <param name="tableMap">Map defining the From-To column combination</param>
        /// <param name="parentColumns">List of parent Columns and pertinent values</param>
        /// <param name="tableToUpdate">Table that we're going to update</param>
        /// <param name="conn">Connetion to target table</param>
        /// <returns>Returns string with the insert SQL statement</returns>
        private static string InsertSqlCmd(TableMap tableMap, List<DataColumn> parentColumns, string tableToUpdate, SqlConnection conn)
        {
            var names = "";
            var values = "";

            //For each column, get both values and compare to each other.  If ANY are different, add to setCmd
            var last = parentColumns.Count - 1;
            var i = 0;
            parentColumns.ForEach(parentColumn =>
            {
                var parentName = parentColumn.Name;
                var parentValue = parentColumn.Value;
                var parentType = parentColumn.ColumnType;

                //Names of columns may be different so get name based on map
                var parentColumnMap = tableMap.ColumnMaps.FirstOrDefault(p => p.FromColumn.Name == parentName);
                var childColumnMap = tableMap.ColumnMaps.FirstOrDefault(p => p.ToColumn.Name == parentName);
                var mappedName = "";
                var isAutoIncremented = false;
                var isKey = false;

                //If parentColumnMap != null it's a parent-to-Child scenario, else it's child-to-parent
                if (parentColumnMap != null)
                {
                    mappedName = parentColumnMap.ToColumn.Name;
                    isAutoIncremented = parentColumnMap.ToColumn.IsAutoNumber;
                    isKey = parentColumnMap.ToColumn.IsKey;
                }
                else if(childColumnMap != null)
                {
                    mappedName = childColumnMap.FromColumn.Name;
                    isAutoIncremented = childColumnMap.FromColumn.IsAutoNumber;
                    isKey = childColumnMap.FromColumn.IsKey;
                }

                //Format values to be used in insert statement
                var comma = i < last ? ", " : "";
                var tick = parentType == typeof(int) || parentType == typeof(long) ? "" : "'";
                var value = tick + parentValue + tick;

                //If it's not auto incremented, we have to take care of generating a key where applicable
                if (!isAutoIncremented)
                {
                    if (isKey)
                        value = GenerateUniqueKey(conn, tableToUpdate, mappedName, value, parentType);

                    names += mappedName + comma;
                    values += value + comma;
                }
                i++;
            });
            
            return $@"INSERT INTO {tableToUpdate} ({names}) VALUES({values})";
        }
        
        /// <summary>
        /// Given a parent and child, this method creates the SQL statement needed to update tables and make both current
        /// and in sync with each other.
        /// </summary>
        /// <param name="tableMap">Map defining the From-To column combination</param>
        /// <param name="parentColumns">List of parent Columns and pertinent values</param>
        /// <param name="childColumns">List of child Columns and pertinent values</param>
        /// <param name="guid">The unique link that's equal between parent and child</param>
        /// <param name="tableToUpdate">Table that we're going to update</param>
        /// <param name="version">The highest RowVersion we're going to update table with</param>
        /// <param name="conn">Connetion to target table</param>
        /// <returns>Returns string with the update SQL statement</returns>
        private static string UpdateSqlCmd(TableMap tableMap, List<DataColumn> parentColumns, List<DataColumn> childColumns, Guid guid, string tableToUpdate, long version, SqlConnection conn)
        {
            var setCmd = "";

            //For each column, get both values and compare to each other.  If ANY are different, add to setCmd
            parentColumns.ForEach(parentColumn =>
            {
                var parentName = parentColumn.Name;
                var parentValue = parentColumn.Value;
                childColumns.ForEach(childColumn =>
                {
                    var childName = childColumn.Name;
                    var childValue = childColumn.Value;

                    //Names of columns may be different so get name based on map
                    var parentColumnMap = tableMap.ColumnMaps.FirstOrDefault(p => p.FromColumn.Name == parentName);
                    var childColumnMap = tableMap.ColumnMaps.FirstOrDefault(p => p.ToColumn.Name == parentName);
                    var mappedName = "";
                    var isAutoIncremented = false;
                    var isKey = false;

                    //If parentColumnMap != null it's a parent-to-Child scenario, else it's child-to-parent
                    if (parentColumnMap != null)
                    {
                        mappedName = parentColumnMap.ToColumn.Name;
                        isAutoIncremented = parentColumnMap.ToColumn.IsAutoNumber;
                        isKey = parentColumnMap.ToColumn.IsKey;
                    }
                    else if (childColumnMap != null)
                    {
                        mappedName = childColumnMap.FromColumn.Name;
                        isAutoIncremented = childColumnMap.FromColumn.IsAutoNumber;
                        isKey = childColumnMap.FromColumn.IsKey;
                    }

                    //If it's not auto-incremented and not column RowVersion (added later), include as part of set.
                    if (!isAutoIncremented && !childName.Equals("RowVersion"))
                    {
                        //If the column names are the same and the values are different we should update that field.
                        if (childName.Equals(mappedName) && !childValue.Equals(parentValue))
                        {
                            var type = childColumn.ColumnType;
                            var value = (type == typeof(int) || type == typeof(long) ? parentValue : "'" + parentValue + "'");

                            //If it's a primary columnName, find a unique value if neccessary
                            if(isKey)
                                value = GenerateUniqueKey(conn, tableToUpdate, mappedName, parentValue.ToString(), type);

                            //Add all else to set
                            setCmd += childName + "=" + value + ", ";
                        }
                    }
                });
            });
            
            return $@"UPDATE {tableToUpdate} 
                      SET {setCmd} RowVersion = {version}
                      WHERE RowGuid='{guid}'";
        }

        /// <summary>
        /// Get's the last sequence number updated during this run
        /// </summary>
        /// <param name="conn">Connection to DB we're getting data from</param>
        /// <param name="syncSchema">Sync schema name</param>
        /// <param name="tableName">Unique table name we're checking against</param>
        /// <returns>Return the sequence number</returns>
        private static long GetLastSequence(SqlConnection conn, string syncSchema, string tableName)
        {
            return (long)new SqlCommand($@"
                    declare @lastSeq bigint
                    select top 1 @lastSeq=LastSequence from {syncSchema}.CleanUpSettings 
                    WHERE TableName='{tableName}'
                    if @lastSeq is null begin
	                    insert {syncSchema}.CleanUpSettings (LastSequence, TableName) values(0, '{tableName}')
	                    select @lastSeq=0
                    end
                    select @lastSeq
                ", conn).ExecuteScalar();
        }

        /// <summary>
        /// Set the sequence number of recent completion for this run
        /// </summary>
        /// <param name="conn">Connection to DB we're setting data to</param>
        /// <param name="syncSchema">Sync schema name</param>
        /// <param name="tableName">Unique table name we're checking against</param>
        /// <param name="lastSequence">Value to update table with</param>
        private static void SetLastSequence(SqlConnection conn, string syncSchema, string tableName, long lastSequence)
        {
            new SqlCommand($@"
                Update {syncSchema}.CleanUpSettings 
                SET LastSequence={lastSequence} 
                WHERE TableName='{tableName}'", conn).ExecuteNonQuery();
        }

        /// <summary>
        /// Used to check if a Guid exists in a table
        /// </summary>
        /// <param name="conn">Connection to table be checked</param>
        /// <param name="value">Guid to check against</param>
        /// <param name="table">Table we're looking at</param>
        /// <returns>True/False</returns>
        private static bool GuidExist(SqlConnection conn, Guid value, string table)
        {
            var count = (int)new SqlCommand($"Select count(*) from {table} where RowGuid='{value}'", conn).ExecuteScalar();
            return count > 0;
        }

        /// <summary>
        /// Used to remove extraneous rows.
        /// </summary>
        /// <param name="conn">Connection used to search for existing rows</param>
        /// <param name="table">Table we're testing against</param>
        /// <param name="rows">Rows we're testing with and removing from</param>
        /// <returns>List of Rows</returns>
        private static List<Row> RemoveExistingRows(SqlConnection conn, string table, List<Row> rows)
        {
            var tempRows = new List<Row>(rows);
            rows.ForEach(p =>
            {
                p.Columns.ForEach(c =>
                {
                    if (c.Name.Equals("RowGuid"))
                    {
                        if (GuidExist(conn, (Guid)c.Value, table))
                            tempRows.Remove(p);
                    }
                });
            });

            return tempRows;
        }

        /// <summary>
        /// Method used to generate a primary key when it's not an auto-incremented field.
        /// </summary>
        /// <param name="conn">Connetion to target table</param>
        /// <param name="table">Table that we're going to update</param>
        /// <param name="columnName">Column that has the PK constraint</param>
        /// <param name="value">Current Column value</param>
        /// <param name="type">Data type of column</param>
        /// <returns>Unique Key</returns>
        private static string GenerateUniqueKey(SqlConnection conn, string table, string columnName, string value, Type type)
        {
            //Check to see if we have current value as a primary key already
            var keyCount = new SqlCommand($@"SELECT count(*) 
                                             FROM {table} 
                                             WHERE {columnName} = {value}", conn).ExecuteScalar();

            //If there's a match, generate a unique key with (HOLDLOCK) which will
            //hopefully stop another task from using our generated key.
            if ((int)keyCount > 0)
            {
                if (type == typeof(int) || type == typeof(long))
                    value = ((int)new SqlCommand($@"SELECT TOP 1 {columnName} FROM {table} WITH (HOLDLOCK) ORDER BY {columnName} DESC", conn).ExecuteScalar() + 1).ToString();
                else
                {
                    var randomString = "";

                    //Try to generate a random string for a set amount of "tries". 
                    //If we find one that isn't used, set value and move on
                    //Else throw an Exception
                    var tries = 1000;
                    for (int i = 0; i < tries; i++)
                    {
                        //Generate random key
                        randomString = GenerateRandomString(value.Replace("'", "").Length);

                        //Check to see if key is there
                        var keys = new SqlCommand($@"SELECT count(*) 
                                                     FROM {table} WITH (HOLDLOCK) 
                                                     WHERE {columnName} = '{randomString}'", conn).ExecuteScalar();

                        //If it isn't there break the loop and continue
                        if ((int)keys == 0) break;

                        //If we've reached the max amount of tries and we STILL couldn't find a unique key, throw an exception
                        //An easy fix would be to increase the length of the random key generated
                        if (i == (tries - 1))
                            throw new Exception($@"Unique key could not be generated within {tries} tries. The last key tried was '{randomString}'. Please try increasing the random key length.");
                    }

                    value = "'" + randomString + "'";
                }
            }

            return value;
        }

        /// <summary>
        /// Generate a random string for unique keys
        /// </summary>
        /// <param name="length">Length of the key to be generated</param>
        /// <returns>The key</returns>
        private static string GenerateRandomString(int length)
        {
            var alpha = new List<string>();

            for (var c = 'A'; c <= 'Z'; c++)
                alpha.Add(Convert.ToString(c));

            var r = new Random();

            var key = "";
            for (var x = 0; x < length; x++)
                key += alpha[r.Next(1, alpha.Count - 1)];

            return key;
        }

        /// <summary>
        /// Returns an initialized Row with a not null RowVersion
        /// </summary>
        /// <returns>Empty Row</returns>
        private static Row GetEmptyRow()
        {
            //EmptyRow used instead of null value IF we have inserts (not updates) to make.
            return new Row
            {
                Columns = new List<DataColumn>() { new DataColumn() { Name = "RowVersion", Value = new long() } }
            };
        }
    }
}