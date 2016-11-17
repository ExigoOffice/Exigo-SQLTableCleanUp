using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Exigo.Services.SQLTableCleanUp
{
    public class Table
    {
        public Table()
        {
            Schema = "dbo";
        }
        public string Schema             { get; set; }
        public string TableName          { get; set; }
        public bool IsMemoryOptimized { get; set; }
        public DateTime LastModifiedDate { get; set; }
        public byte[] LastModifiedHash   { get; set; }

        //used for id increment logs
        public int LastID { get; set; }

        public string Join { get; set; }
        public string Where { get; set; }
        public string InvalidationTables { get; set; }

        public string SchemaQualifiedTableName
        {
            get {  return Schema + "." + TableName;}
        }

        
        public override string ToString()
        {
            return Schema + "." + TableName;
        }
    }
}