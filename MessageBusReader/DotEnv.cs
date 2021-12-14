namespace MessageBusReader
{
    using System;
    using System.IO;

    // https://dusted.codes/dotenv-in-dotnet
    public static class DotEnv
    {
        public static void Load(string filePath)
        {
            if (!File.Exists(filePath))
            {
                throw new FileNotFoundException(".env file not found", filePath);
            }

            foreach (var line in File.ReadAllLines(filePath))
            {
                var positionOfEquals = line.IndexOf('=');
                if (positionOfEquals > -1)
                { 
                    var name = line.Substring(0, positionOfEquals);
                    var value = line.Substring(positionOfEquals + 1);

                    Environment.SetEnvironmentVariable(name, value);
                }
            }
        }
    }
}
