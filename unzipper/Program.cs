﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace unzipper
{
    class Program
    {
        static void Main(string[] args)
        {
            //We share the same EXE for both the main program and the task
            //Decide which one to start based on the command line parameters
            if (args != null && args.Length > 0 && args[0] == "--Task")
            {
                UnzipperTask.TaskMain(args);
            }
            else
            {
                Job.JobMain(args);
            }
        }
    }
}
