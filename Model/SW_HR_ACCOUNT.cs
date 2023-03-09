using Dapper.Contrib.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lib.Sync.Model
{
    [Table("SW_HR_ACCOUNT")]
    public class SW_HR_ACCOUNT
    {
        public string Account { get; set; }
        public string Name { get; set; }
        public string TitleName { get; set; }
        public string FunctionName { get; set; }
        public string Email { get; set; }
        public string Department { get; set; }
        public string Dept_Code { get; set; }
        public string Sex { get; set; }
        public string AD_Account { get; set; }
        public string Location { get; set; }
        public string TimeZone { get; set; }
        public string BirthDay { get; set; }
        public string ArriveDay { get; set; }
        public string EmployeeState { get; set; }
    }
}
