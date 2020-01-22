using HeroesProfile_Backend.Models;

namespace HeroesProfile_Backend
{
    public static class ConnectionStringBuilder
    {
        public static string BuildConnectionString(DbSettings dbSettings)
        {
           return $"SERVER={dbSettings.server};DATABASE={dbSettings.database};UID={dbSettings.database_user};PASSWORD={dbSettings.database_password};Charset=utf8;";
        
        }
        public static string BuildDevConnectionString(DbSettings dbSettings)
        {
            return $"SERVER={dbSettings.server_dev};DATABASE={dbSettings.database_dev};UID={dbSettings.database_user_dev};PASSWORD={dbSettings.database_password_dev};Charset=utf8;";
        
        }
    }
}