using Ede.Uof.Utility.Task;
using Ede.Uof.Utility.Log;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lib.Sync.Sync
{
    public class SyncTask : BaseTask
    {
        public override void Run(params string[] args)
        {
            try
            {
                SyncUCO uco = new SyncUCO();
                if (args.Length > 0)
                {
                    Logger.Write("Sync", "手動開始");

                    uco.RunSyncTask(args[0]);
                }
                else
                {
                    Logger.Write("Sync", "排程開始");

                    uco.RunSyncTask("排程");
                }

                uco.ClearAllCache();

            }
            catch (Exception ex)
            {
                Logger.Write("Sync", ex.Message);
            }
        }
    }
}
