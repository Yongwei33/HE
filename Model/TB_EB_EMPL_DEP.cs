using Dapper.Contrib.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lib.Sync.Model
{
    [Table("TB_EB_EMPL_DEP")]
    public class TB_EB_EMPL_DEP
    {
        public string USER_GUID { get; set; }
        public string GROUP_ID { get; set; }
        public string TITLE_ID { get; set; }
        public int? ORDERS { get; set; }

    }
}
