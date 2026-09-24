using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace shared.Logging
{
    public class FileLogger
    {
        private readonly string _filePath;
        private readonly object _lock = new();

        public FileLogger(string filePath)
        { 
            _filePath = filePath;
            string? dir = Path.GetDirectoryName(filePath);
            if (!string.IsNullOrEmpty(dir))
            {
                Directory.CreateDirectory(dir);
            }
        }
        public void Log(string layer, string level, string message)
        {
            string line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [{layer}] [{level}] {message}";
            
            lock (_lock)
            {
                Console.WriteLine(line);
                File.AppendAllText(_filePath, line + Environment.NewLine);
            }
        }
    }
}
