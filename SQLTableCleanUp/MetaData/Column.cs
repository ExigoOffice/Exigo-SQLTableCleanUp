using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Exigo.Services.SQLTableCleanUp
{
    public enum DataType
    {
        Short       = 48,
        Int         = 56,
        Long        = 127,
        DateTime    = 61,
        Decimal     = 60,
        Boolean     = 104, 
        String      = 231,
        Binary      = 165,
        Guid        = 36
    }

    public class Column
    {
        public string Name       { get; set; }
        public DataType Type     { get; set; }
        public int Size          { get; set; }
        public int MemoryOptimizedSize { get; set; }
        public bool IsKey        { get; set; }
        public string Expression { get; set; }
        public bool IsAutoNumber { get; set; }

        public string Default    { get; set; }

        //we have this nullable so the system can decide for when it is not set
        public bool? AllowDbNull { get; set; }

        //Allows this column to be added gracefully to a table with default values without triggering a trueup
        public bool AllowDefaultAdd { get; set; }

        public int GetMemoryOptimizedSize()
        {
            //--> start with column size
            int size = this.Size;

            //--> If we are an nvarchar(max) set to max of 2000
            if (size <= 0) size = 2000;

            //--> If we have a memory optimized override use that instead
            if (this.MemoryOptimizedSize > 0 && this.MemoryOptimizedSize < size)
                size = this.MemoryOptimizedSize;

            return size;
        }
    }
}
