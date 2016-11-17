using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Exigo.Services.SQLTableCleanUp
{
    public class IndexColumn
    {
        public string Name { get; set; }
        public bool IsDescending { get; set; }

        public override string ToString()
        {
            return Name;
        }

    }

    public class Index
    {
        public Index()
        {
            Columns = new List<IndexColumn>();
        }

        public string Name { get; set; }
        public List<IndexColumn> Columns { get; set; }
        public bool IsUnique { get; set; }

        public bool IsClustered { get; set; }
        public override string ToString()
        {

            var s = "";
            foreach (var c in Columns)
            {
                if (s!="")
                    s = "," + s;
              
                s = s + c.Name;
                if (c.IsDescending)
                    s =s + " desc";
            }

            return s;

        }
    }
}
