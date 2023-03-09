using Dapper.Contrib.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lib.Sync.Model
{
    [Table("SW_HR_JOBTITLE")]
    public class SW_HR_JOBTITLE
    {
        public string Code { get; set; }
        public string Name { get; set; }
        public string Level { get; set; }
    }
}
