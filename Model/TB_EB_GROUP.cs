using Dapper.Contrib.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lib.Sync.Model
{
    [Table("TB_EB_GROUP")]
    public class TB_EB_GROUP
    {
        public string GROUP_ID { get; set; }
        public string GROUP_TYPE { get; set; }
        public string GROUP_NAME { get; set; }
        public string PARENT_GROUP_ID { get; set; }
        public int LFT { get; set; }
        public int RGT { get; set; }
        public int LEV { get; set; }
        public string GROUP_CODE { get; set; }
        public bool? ACTIVE { get; set; }
        public string COMPANY_ID { get; set; }

    }
}
