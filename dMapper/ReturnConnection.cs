using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Common;

namespace dMapper
{
    public class ReturnConnection
    {
        public DbConnection Connection { get; set; }
        public bool DisposeConnection { get; set; }
    }
}