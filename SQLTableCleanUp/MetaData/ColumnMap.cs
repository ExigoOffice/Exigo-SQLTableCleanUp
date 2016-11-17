using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Exigo.Services.SQLTableCleanUp
{
    public class ColumnMap
    {
        public ColumnMap()
        {
            FromColumn  = new Column();
            ToColumn    = new Column();
        }

        public Column FromColumn { get; set; }
        public Column ToColumn { get; set; }
    }
}
