using receivers.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace receivers.Implementation
{
    public class EffectStore : IEffectStore
    {
        private readonly String _path;
        private readonly HashSet<Guid> _seen = new();
        private readonly Lock _lock = new Lock();
        public EffectStore(string path)
        {
            _path = path;
            if (File.Exists(path))
            {
                foreach (var line in File.ReadLines(path))
                {
                    if (TryParseMessageId(line, out var id))
                    {
                        _seen.Add(id);
                    }
                }
            }
        }
        private static bool TryParseMessageId(string line, out Guid id)
        {
            id = default;
            var parts = line.Split('|',2);
            return parts.Length > 0 && Guid.TryParse(parts[0], out id);
        }
        public bool ApplyOnce(Guid messageId,string content)
        {
            lock (_lock)
            {
                if (_seen.Contains(messageId))
                {
                    return false;
                }
                var sanitized = (content ?? string.Empty).Replace("|", " ").Replace("\n", " ").Replace("\r", " ");
                var line = $"{messageId}|{DateTime.UtcNow:o}|{sanitized}";
                File.AppendAllText(_path, line + Environment.NewLine);

                _seen.Add(messageId);
                return true;
            }
        }
    }
}
