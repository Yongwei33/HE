using Dapper.Contrib.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lib.Sync.Model
{
    [Table("SW_HR_CONCURREN")]
    public class SW_HR_CONCURREN
    {
        public string Account { get; set; }
        public string PartJobDepCode { get; set; }
        public string PartJobDepName { get; set; }
        public string PartJobJobTitleName { get; set; }
        public string PartJobFunctionName { get; set; }
        public string BeginDate { get; set; }
        public string EndDate { get; set; }
        public string IsEffective { get; set; }
    }
}
