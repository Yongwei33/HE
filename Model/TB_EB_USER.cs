using Dapper.Contrib.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lib.Sync.Model
{
    [Table("TB_EB_USER")]
    public class TB_EB_USER
    {
        public string USER_GUID { get; set; }
        public string ACCOUNT { get; set; }
        public string NAME { get; set; }
        public bool IS_SUSPENDED { get; set; } //0:false, 1:true
    }
}
