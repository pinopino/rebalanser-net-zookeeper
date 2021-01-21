using System.IO;
using System.Runtime.Serialization.Json;
using System.Text;

namespace Rebalanser.Common
{
    public static class JSONSerializer<T>
        where T : class
    {
        /// <summary>
        /// Serializes an object to JSON
        /// </summary>
        public static string Serialize(T instance)
        {
            var serializer = new DataContractJsonSerializer(typeof(T));
            using (var stream = new MemoryStream())
            {
                serializer.WriteObject(stream, instance);
                var ser = Encoding.UTF8.GetString(stream.ToArray());
                return ser;
            }
        }

        /// <summary>
        /// DeSerializes an object from JSON
        /// </summary>
        public static T DeSerialize(string json)
        {
            if (string.IsNullOrWhiteSpace(json))
                return default(T);

            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(json)))
            {
                stream.Position = 0;
                var serializer = new DataContractJsonSerializer(typeof(T));
                return serializer.ReadObject(stream) as T;
            }
        }
    }
}
