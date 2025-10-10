using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using System.Text.Json;
using System.Threading.Tasks;

namespace StockIntegrity.Helpers
{
    public static class EncryptionService
    {
        [Required]
        private static readonly string keyEnvVar = Environment.GetEnvironmentVariable("AES_ENCRYPTION_KEY");
        [Required]
        private static readonly string ivEnvVar = Environment.GetEnvironmentVariable("AES_ENCRYPTION_IV");

        private static byte[] Key => Encoding.UTF8.GetBytes(keyEnvVar.PadRight(32).Substring(0, 32)); // Ensure 32 bytes for AES-256
        private static byte[] IV => Encoding.UTF8.GetBytes(ivEnvVar.PadRight(16).Substring(0, 16));   // Ensure 16 bytes for AES

        public static string Encrypt(string plainText)
        {
            if (string.IsNullOrEmpty(keyEnvVar) || string.IsNullOrEmpty(ivEnvVar))
                throw new InvalidOperationException("Encryption key or IV not set in environment variables.");

            using (Aes aes = Aes.Create())
            {
                aes.Key = Key;
                aes.IV = IV;

                ICryptoTransform encryptor = aes.CreateEncryptor(aes.Key, aes.IV);
                using (MemoryStream ms = new MemoryStream())
                using (CryptoStream cs = new CryptoStream(ms, encryptor, CryptoStreamMode.Write))
                using (StreamWriter sw = new StreamWriter(cs))
                {
                    sw.Write(plainText);
                    sw.Close();
                    return ("!==ENC==!" + Convert.ToBase64String(ms.ToArray()));

                }
            }
        }

        public static string Decrypt(string encryptedText)
        {
            //Remove the !==ENC==! prefix
            encryptedText = encryptedText.Substring(9);
            if (string.IsNullOrEmpty(keyEnvVar) || string.IsNullOrEmpty(ivEnvVar))
                throw new InvalidOperationException("Encryption key or IV not set in environment variables.");

            using (Aes aes = Aes.Create())
            {
                aes.Key = Key;
                aes.IV = IV;

                ICryptoTransform decryptor = aes.CreateDecryptor(aes.Key, aes.IV);
                byte[] buffer = Convert.FromBase64String(encryptedText);

                using (MemoryStream ms = new MemoryStream(buffer))
                using (CryptoStream cs = new CryptoStream(ms, decryptor, CryptoStreamMode.Read))
                using (StreamReader sr = new StreamReader(cs))
                {
                    return sr.ReadToEnd();
                }
            }
        }

        public static bool IsEncrypted(string input)
        {
            if (input.Contains("!==ENC==!"))
            {
                return true;
            }
            else
            {
                return false;
            }
        }


        public static void UpdateConfigFile(string encryptedConnectionString, string connStringName)
        {
            string configFilePath = AppContext.BaseDirectory + "appsettings.json";

            // Read existing JSON content
            string jsonContent = File.ReadAllText(configFilePath);

            // Parse the JSON
            var jsonObject = JsonNode.Parse(jsonContent);

            // Split the connStringName by ':' to navigate through nested JSON properties
            string[] keys = connStringName.Split(':');
            JsonNode currentNode = jsonObject;

            // Traverse to the second-last key in the path
            for (int i = 0; i < keys.Length - 1; i++)
            {
                // Check if the current key exists
                if (currentNode[keys[i]] == null)
                {
                    throw new Exception($"Key '{keys[i]}' not found in config.");
                }

                currentNode = currentNode[keys[i]];
            }

            // Update the final key with the encrypted connection string
            currentNode[keys[^1]] = encryptedConnectionString;

            // Write back to config.json
            string updatedJson = JsonSerializer.Serialize(jsonObject, new JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText(configFilePath, updatedJson);
        }
    }
}
