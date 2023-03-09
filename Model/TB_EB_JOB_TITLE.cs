using Dapper.Contrib.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lib.Sync.Model
{
    [Table("TB_EB_JOB_TITLE")]
    public class TB_EB_JOB_TITLE
    {
        public int? RANK { get; set; }
        public string TITLE_ID { get; set; }
        public string TITLE_NAME { get; set; }
        public int? MAX_FILE_SIZE { get; set; }


    }
}
