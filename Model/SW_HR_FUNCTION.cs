using Dapper.Contrib.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lib.Sync.Model
{
    [Table("SW_HR_FUNCTION")]
    public class SW_HR_FUNCTION
    {
        public string Code { get; set; }
        public string Name { get; set; }
    }
}
