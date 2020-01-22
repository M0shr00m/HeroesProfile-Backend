namespace HeroesProfile_Backend.Models
{
    public class ReplaysNotProcessed
    {
        public string replayID { get; set; }
        public string region { get; set; }
        public string game_type { get; set; }
        public string game_length { get; set; }
        public string game_date { get; set; }
        public string game_map { get; set; }
        public string game_version { get; set; }
        public string size { get; set; }
        public string date_parsed { get; set; }
        public string count_parsed { get; set; }
        public string url { get; set; }
        public string failure_status { get; set; }
        public string processed { get; set; }
    }
}
