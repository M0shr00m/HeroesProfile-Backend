namespace HeroesProfile_Backend
{
    internal class DB_Connect
    {
        //Previously where production config data sat.  Likely need to switch this to pulling in the data from a config file, or set it in the ENV and pull there.
        public string heroesprofile_config =
          "SERVER=localhost;" +
          "DATABASE=heroesprofile;" +
          "UID=root;" +
          "PASSWORD=;" +
          "Charset=utf8;";
    }
}
