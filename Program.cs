using System;
using System.IO;

namespace FileCompressor
{
    class Program
    {
        static void Main(string[] args)
        {
            //GZipTest.exe compress / decompress[имя исходного файла][имя результирующего файла]
            //Example
            //GZipTest.exe compress "D:\Folder\SubFolder\MyFile.mkv" "D:\Folder\SubFolder\MyFile.mkv.gz"

            if (args == null || args.Length != 3)
            {
                Console.WriteLine("Request is wrong");
                Console.WriteLine("Press Any Key to exit");
                Console.ReadKey();
                return;
            }

            GZipCompressor compressor = new GZipCompressor();

            if (args[0] == "compress")
            {
                if (CheckFiles(args))
                    compressor.Compress(args[1], args[2]);

            }
            else if (args[0] == "decompress")
            {
                if (CheckFiles(args))
                    compressor.Decompress(args[1], args[2]);
            }
            else
            {
                Console.WriteLine("Request is wrong");
            }
            compressor = null;
            Console.WriteLine("Press Any Key to exit");
            Console.ReadKey();
        }

        public static bool CheckFiles(string[] args)
        {
            if (!File.Exists(args[1]))
            {
                Console.WriteLine("Source file not found");
                return false;
            }

            if (File.Exists(args[2]))
            {
                Console.WriteLine("Output file exist");
                return false;
            }
            return true;
        }
    }


}
