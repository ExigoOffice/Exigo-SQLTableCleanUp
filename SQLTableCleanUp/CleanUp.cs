using Exigo.Services.SQLTableCleanUp;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Windows.Forms;
using System.IO;
using Newtonsoft.Json;

namespace SQLTableCleanUp
{
    public partial class CleanUp : Form
    {
        public class SyncConfig
        {
            public int CompanyID { get; set; }
            public string Name { get; set; }
            public string ConnectionString { get; set; }
            public string DestinationString { get; set; }
            public bool AllowSensitiveData { get; set; }
            public string ExcludedSchemas { get; set; }
            public bool AllowMergeWrite { get; set; }
            public DateTime? TrueupDate { get; set; }
            public bool AllowJobs { get; set; }
            public bool AllowTransferMeter { get; set; }
            public bool AllowTransferBilling { get; set; }
            public string ParentSync { get; set; }
            public string ChildSync { get; set; }
        }

        public CleanUp()
        {
            InitializeComponent();
        }

        public SyncConfig LoadJson(string jsonFile)
        {
            using (StreamReader r = new StreamReader(jsonFile))
            {
                string json = r.ReadToEnd();
                return JsonConvert.DeserializeObject<SyncConfig>(json);
            }
        }

        private void cleanupBtn_Click(object sender, EventArgs e)
        {
            var items = LoadJson("C:\\Program Files\\Exigo\\SQLCleanUpUtil\\ConfigData.json");

            using (var parent = new SqlConnection(items.ConnectionString))
            using (var child1 = new SqlConnection(items.DestinationString + "App=Sync"))
            {
                parent.Open();
                child1.Open();

                var config = new SyncConfig
                {
                    AllowSensitiveData = items.AllowSensitiveData,
                    CompanyID = items.CompanyID,
                    ConnectionString = items.ConnectionString,
                    ExcludedSchemas = items.ExcludedSchemas,
                    Name = items.Name,
                    AllowJobs = items.AllowJobs
                };

                //start up new jobs for new connection strings not in here
                var tableMaps = InternalGetTableMaps(parent, config);

                var syncReport = "";
                var count = SqlTableCleanUpUtil.CleanUpTables(tableMaps, parent, child1, items.ParentSync, items.ChildSync, reportOnlyMode.Checked, out syncReport);
                outputBox.AppendText(syncReport);
                outputBox.AppendText($"{count} total differences were found.\n\n");
            }
        }

        //used for testing
        public string FilterTable { get; set; }

        internal IEnumerable<TableMap> InternalGetTableMaps(SqlConnection conn, SyncConfig config)
        {
            var tableMaps = new List<TableMap>();
            var _excludedSchemas = config.ExcludedSchemas;
            var _companyID = config.CompanyID;

            var cmd = new SqlCommand(@"
                select  SchemaName          = ee.SchemaName, 
                        EntityName          = ee.EntityName, 
                        EntitySetName      = ee.EntitySetName,
                        PropertyName        = c.Name, 
                        c.system_type_id, 
                        c.is_nullable, 
                        c.is_identity, 
                        is_key = cast((case when ic.object_id is null then 0 else 1 end) as bit),
                        ee.DbSchema,
                        ee.Navigations,
                        c.max_length,
                        dc.definition,
                        ee.IdentityRangeIncrement
                from sys.columns c
                inner join sys.tables t
                    on c.object_id = t.object_id
                inner join sys.schemas s
                    on t.schema_id = s.schema_id
                inner join ExtendedEntity ee
		            on ee.EntityName = t.Name collate catalog_default
		            and ee.dbSchema = s.Name collate catalog_default
                left join sys.indexes i
                    on i.object_id = t.object_id
                    and i.is_primary_key=1
                left join sys.index_columns ic
                    on ic.object_id = i.object_id
                    and ic.index_id = i.index_id
                    and ic.column_id = c.column_id
                left join sys.default_constraints dc
					on dc.parent_object_id = c.object_id
					and dc.parent_column_id = c.column_id
                where   ee.CompanyID    = @CompanyID "
                + ((string.IsNullOrEmpty(FilterTable)) ? " and SyncTypeID=2 " : " and c.object_id in (object_id('" + FilterTable.Replace(",", "'),object_id('") + "'))")
                + (string.IsNullOrEmpty(_excludedSchemas) ? "" : " and ee.SchemaName not in ('" + _excludedSchemas.Replace(",", "','") + "') ")
                + @" order by ee.schemaname, ee.EntityName, c.column_id", conn);

            cmd.Parameters.Add("@CompanyID", SqlDbType.Int).Value = _companyID;


            var rd = cmd.ExecuteReader();
            var e = new TableMap();

            while (rd.Read())
            {
                if (e.ToTable.Schema != rd.GetString(0) || e.ToTable.TableName != rd.GetString(2))
                {

                    e = new TableMap();
                    e.FromTable.Schema = rd.GetString(8);
                    e.ToTable.Schema = rd.GetString(0);

                    e.FromTable.TableName = rd.GetString(1);  //we store singular on exigo side
                    e.ToTable.TableName = rd.GetString(2);  //we want plural on dest side to go with odata

                    e.IdentityRangeIncrement = rd.GetInt32(12);

                    tableMaps.Add(e);
                }

                var col = new Column
                {
                    Name = rd.GetString(3),
                    IsKey = rd.GetBoolean(7),
                    Type = (DataType)Convert.ToInt32(rd[4]),
                    IsAutoNumber = rd.GetBoolean(6),
                    AllowDbNull = rd.GetBoolean(5)
                };

                if (col.Type == DataType.String)
                {
                    var sz = rd.GetInt16(10);
                    if (sz > 0)
                    {
                        //--> reduce by half if it is unicode
                        if (rd.GetByte(4) == 239 || rd.GetByte(4) == 231) sz = Convert.ToInt16(sz / 2);
                        col.Size = sz;
                    }

                    if (col.Size == 0) col.Size = -1;
                }
                else if (col.Type == DataType.Binary)
                {
                    col.Size = rd.GetInt16(10);
                    if (col.Size == -1) col.Size = 0;
                }

                if (!rd.IsDBNull(11))
                {
                    string df = rd.GetString(11);
                    if (df.StartsWith("(")) df = df.Substring(1);
                    if (df.EndsWith(")")) df = df.Substring(0, df.Length - 1);
                    col.Default = df;
                }



                e.ColumnMaps.Add(new ColumnMap { FromColumn = col, ToColumn = col });
            }

            rd.Close();


            return tableMaps;
        }

        private void clearBtn_Click(object sender, EventArgs e)
        {
            outputBox.Clear();
        }
    }
}
