using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Exigo.Services.SQLTableCleanUp
{
    /* We need a way to 
     *
     *  a) See if a table needs a companyID filter
     *  b) See if the table has a ModifiedDate we can use
     * 
     * We'll need to evolve this....
     * 
     * 
     * 
     */

    public enum SyncType
    {
        ReplaceTable,
        ReplaceTableMemory,
        ChangedRows,
        AppendOnly,
    }

    public class TableMap
    {
        public TableMap()
        {
            FromTable       = new Table();
            ToTable         = new Table();
            ColumnMaps      = new List<ColumnMap>();
            SyncType        = SyncType.ReplaceTable;
            ChildTables     = new List<TableMap>();
            Indexes         = new List<Index>();
            Views           = new List<View>();
            StoredProcedures= new List<StoredProcedure>();
            IdentityRangeIncrement = 100000;
        }

        public int Version { get; set; }

        //I don't like this here 
        public bool HasCompanyID { get; set; }

        public string AppendOnlyFieldName { get; set; }

        public bool EnableDeleteDetection { get; set; }

        public string ModifiedDateField { get; set; }

        public int IdentityRangeIncrement { get; set; }


        public SyncType SyncType { get; set; }
        public Table FromTable { get; set; }
        public Table ToTable { get; set; }

        public List<TableMap> ChildTables { get; set; }
        public List<ColumnMap> ColumnMaps { get; set; }
        public List<Index> Indexes { get; set; }
        public List<View> Views { get; set; }
        public List<StoredProcedure> StoredProcedures { get; set; }

        public IEnumerable<Column> FromColumns 
        {
            get
            {
                foreach (var colMap in ColumnMaps)
                {
                    yield return colMap.FromColumn;
                }
            }
        }

        public IEnumerable<Column> ToColumns 
        {
            get
            {
                foreach (var colMap in ColumnMaps)
                {
                    yield return colMap.ToColumn;
                }
            }
        }

        public IEnumerable<Column> ColumnsForTable(Table table)
        {
            if (table==FromTable)
                return FromColumns;
            else if (table==ToTable)
                return ToColumns;
            else 
                throw new InvalidOperationException();
                     
        }

        public override string ToString()
        {
            return FromTable.ToString() + " -> " + ToTable.ToString();
        }

    }
}
