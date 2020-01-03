using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using MySql.Data.MySqlClient;
using System.Net;
using System.IO;

using System.Text.RegularExpressions;
namespace HeroesProfile_Backend
{
    class GrabHotsAPIData
    {
        private string db_connect_string = new DB_Connect().heroesprofile_config;
        private Dictionary<string, string> heroes = new Dictionary<string, string>();
        private Dictionary<string, string> heroes_translations = new Dictionary<string, string>();
        private Dictionary<string, string> heroes_alt = new Dictionary<string, string>();
        private Dictionary<string, string> heroes_attr = new Dictionary<string, string>();
        private Dictionary<string, string> role = new Dictionary<string, string>();
        private Dictionary<string, string> maps = new Dictionary<string, string>();
        private Dictionary<string, string> maps_short = new Dictionary<string, string>();
        private Dictionary<string, string> maps_translations = new Dictionary<string, string>();
        private Dictionary<string, string> game_types = new Dictionary<string, string>();
        private Dictionary<string, string> talents = new Dictionary<string, string>();
        private Dictionary<string, string> mmr_ids = new Dictionary<string, string>();
        private Dictionary<string, string> league_tiers = new Dictionary<string, string>();
        private Dictionary<string, DateTime[]> seasons = new Dictionary<string, DateTime[]>();
        private Dictionary<string, string> seasons_game_versions = new Dictionary<string, string>();
        private Dictionary<string, ReplaysNotProcessed> notProcessedReplays = new Dictionary<string, ReplaysNotProcessed>();

        private Dictionary<long, HotsApiJSON.ReplayData> replays_to_run = new Dictionary<long, HotsApiJSON.ReplayData>();



        private ConcurrentDictionary<long, ParseStormReplay> replayData_grabbed = new ConcurrentDictionary<long, ParseStormReplay>();

        public GrabHotsAPIData()
        {
            int maxValue = 0;
            using (MySqlConnection conn = new MySqlConnection(db_connect_string))
            {
                conn.Open();


                using (MySqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT tier_id, name FROM league_tiers";
                    MySqlDataReader Reader = cmd.ExecuteReader();

                    while (Reader.Read())
                    {
                        league_tiers.Add(Reader.GetString("name"), Reader.GetString("tier_id"));
                    }
                }

                using (MySqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT id, name, new_role FROM heroes";
                    MySqlDataReader Reader = cmd.ExecuteReader();

                    while (Reader.Read())
                    {
                        heroes.Add(Reader.GetString("name"), Reader.GetString("id"));
                        role.Add(Reader.GetString("name"), Reader.GetString("new_role"));
                    }
                }

                using (MySqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT type_id, name FROM game_types";
                    MySqlDataReader Reader = cmd.ExecuteReader();

                    while (Reader.Read())
                    {
                        game_types.Add(Reader.GetString("name").Replace(" ", ""), Reader.GetString("type_id"));
                    }
                }
                using (MySqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT * FROM heroesprofile.heroes_translations;";
                    MySqlDataReader Reader = cmd.ExecuteReader();

                    while (Reader.Read())
                    {
                        if (!heroes_translations.ContainsKey(Reader.GetString("translation").ToLower()))
                        {
                            heroes_translations.Add(Reader.GetString("translation").ToLower(), Reader.GetString("name"));

                        }
                    }
                }
                using (MySqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT map_id, name, short_name FROM maps";
                    MySqlDataReader Reader = cmd.ExecuteReader();

                    while (Reader.Read())
                    {
                        maps.Add(Reader.GetString("name"), Reader.GetString("map_id"));
                        maps_short.Add(Reader.GetString("short_name"), Reader.GetString("name"));

                    }
                }

                using (MySqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT name, translation FROM maps_translations";
                    MySqlDataReader Reader = cmd.ExecuteReader();

                    while (Reader.Read())
                    {
                        maps_translations.Add(Reader.GetString("translation"), Reader.GetString("name"));
                    }
                }




                using (MySqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT hero_name, talent_id, talent_name FROM heroes_data_talents order by talent_id asc";
                    MySqlDataReader Reader = cmd.ExecuteReader();

                    while (Reader.Read())
                    {
                        string hero = Reader.GetString("hero_name");
                        string talent = Reader.GetString("talent_name");
                        string[] split = Regex.Split(talent, @"(?<!^)(?=[A-Z])");

                        if (hero == "" && split.Length > 0)
                        {
                            hero = split[0];
                        }
                        if (!talents.ContainsKey(Reader.GetString("hero_name") + "|" + Reader.GetString("talent_name")))
                        {
                            talents.Add(Reader.GetString("hero_name") + "|" + Reader.GetString("talent_name"), Reader.GetString("talent_id"));

                        }
                    }
                }


                using (MySqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT name, alt_name, short_name, attribute_id FROM heroes";
                    MySqlDataReader Reader = cmd.ExecuteReader();

                    while (Reader.Read())
                    {
                        if (!heroes_alt.ContainsKey(Reader.GetString("name")))
                        {
                            string hero = Reader.GetString("name");
                            string alt = Reader["alt_name"].Equals(DBNull.Value) ? String.Empty : Reader.GetString("alt_name");

                            if (alt == "")
                            {
                                alt = Reader.GetString("short_name");
                                alt = char.ToUpper(alt.First()) + alt.Substring(1).ToLower();

                            }
                            heroes_alt.Add(alt, hero);

                            heroes_attr.Add(Reader.GetString("attribute_id"), Reader.GetString("name"));
                        }
                    }
                }




                using (MySqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT mmr_type_id, name FROM mmr_type_ids";
                    MySqlDataReader Reader = cmd.ExecuteReader();

                    while (Reader.Read())
                    {
                        mmr_ids.Add(Reader.GetString("name"), Reader.GetString("mmr_type_id"));
                    }
                }

                using (MySqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT id, start_date, end_date FROM season_dates;";
                    MySqlDataReader Reader = cmd.ExecuteReader();


                    while (Reader.Read())
                    {
                        DateTime[] dates = new DateTime[2];

                        dates[0] = DateTime.Parse(Reader.GetString("start_date"));
                        dates[1] = DateTime.Parse(Reader.GetString("end_date"));


                        seasons.Add(Reader.GetString("id"), dates);
                    }
                }

                using (MySqlCommand cmd = conn.CreateCommand())
                {

                    cmd.CommandText = "SELECT * FROM replays_not_processed WHERE count_parsed < 3 ORDER BY replayID ASC LIMIT 100";
                    MySqlDataReader Reader = cmd.ExecuteReader();

                    while (Reader.Read())
                    {
                        string replayID = Reader["replayID"].Equals(DBNull.Value) ? String.Empty : Reader.GetString("replayID");
                        if ((Reader["replayID"].Equals(DBNull.Value) ? String.Empty : Reader.GetString("replayID")) != "")
                        {

                            ReplaysNotProcessed ron = new ReplaysNotProcessed();

                            ron.replayID = Reader.GetString("replayID");
                            ron.region = Reader.GetString("region");
                            ron.game_type = Reader.GetString("game_type");
                            ron.game_length = Reader.GetString("game_length");
                            ron.game_date = Reader.GetString("game_date");
                            ron.game_map = Reader.GetString("game_map");
                            ron.game_version = Reader.GetString("game_version");
                            ron.size = Reader["size"].Equals(DBNull.Value) ? String.Empty : Reader.GetString("size");
                            ron.date_parsed = Reader["date_parsed"].Equals(DBNull.Value) ? String.Empty : Reader.GetString("date_parsed");
                            ron.count_parsed = Reader.GetString("count_parsed");
                            ron.url = Reader.GetString("url");
                            ron.failure_status = Reader["failure_status"].Equals(DBNull.Value) ? String.Empty : Reader.GetString("failure_status");
                            ron.processed = Reader.GetString("processed");




                            notProcessedReplays.Add(replayID, ron);

                        }

                    }
                }


                using (MySqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT season, game_version FROM season_game_versions";
                    MySqlDataReader Reader = cmd.ExecuteReader();

                    while (Reader.Read())
                    {
                        if (!seasons_game_versions.ContainsKey(Reader.GetString("game_version")))
                        {
                            seasons_game_versions.Add(Reader.GetString("game_version"), Reader.GetString("season"));

                        }
                    }
                }

                using (MySqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT MAX(replayID) as max_replayID FROM replay";
                    MySqlDataReader Reader = cmd.ExecuteReader();

                    while (Reader.Read())
                    {
                        maxValue = Convert.ToInt32(Reader.GetString("max_replayID"));
                    }
                }
                runNotProccessed();


                int not_processed_maxValue = 0;

                using (MySqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT MAX(replayID) as max_replayID FROM replays_not_processed";
                    MySqlDataReader Reader = cmd.ExecuteReader();

                    if (Reader.HasRows)
                    {
                        while (Reader.Read())
                        {
                            string value = Reader["max_replayID"].Equals(DBNull.Value) ? String.Empty : Reader.GetString("max_replayID");
                            if (value != "")
                            {
                                not_processed_maxValue = Convert.ToInt32(Reader.GetString("max_replayID"));

                            }
                        }
                    }

                }
                int brawl_maxValue = 0;
                using (MySqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT if(MAX(replayID) is null, 0, MAX(replayID)) as max_replayID FROM heroesprofile_brawl.replay";
                    MySqlDataReader Reader = cmd.ExecuteReader();

                    while (Reader.Read())
                    {
                        brawl_maxValue = Convert.ToInt32(Reader.GetString("max_replayID"));
                    }
                }


                maxValue++;
                if (not_processed_maxValue > maxValue)
                {
                    maxValue = not_processed_maxValue;

                    if (brawl_maxValue > maxValue)
                    {
                        maxValue = brawl_maxValue;
                    }
                }
                else if (brawl_maxValue > maxValue)
                {
                    maxValue = (brawl_maxValue + 1);
                }
                //maxValue = 15757424;
                recurseHotsApiCall(maxValue);
            }
        }
        private void runNotProccessed()
        {
            Parallel.ForEach(
                notProcessedReplays.Keys,
                //new ParallelOptions { MaxDegreeOfParallelism = -1 },
                //new ParallelOptions { MaxDegreeOfParallelism = 1 },
                new ParallelOptions { MaxDegreeOfParallelism = 100 },
                item =>
                {
                    //
                    Console.WriteLine("Running Reply: " + item);
                    ParseStormReplay p = new ParseStormReplay(Convert.ToInt64(item), new Uri(notProcessedReplays[item].url, UriKind.Absolute), notProcessedReplays[item], maps, maps_translations, game_types, talents, seasons_game_versions, mmr_ids, seasons, heroes, heroes_translations, maps_short, mmr_ids, role, heroes_attr);
                    replayData_grabbed.TryAdd(Convert.ToInt64(item), p);

                });

            SortedDictionary<long, ParseStormReplay> sorted_replayData_grabbed = new SortedDictionary<long, ParseStormReplay>();

            foreach (var item in replayData_grabbed.Keys)
            {
                sorted_replayData_grabbed.Add(item, replayData_grabbed[item]);
            }

            Parallel.ForEach(
                sorted_replayData_grabbed.Keys,
                //new ParallelOptions { MaxDegreeOfParallelism = -1 },
                //new ParallelOptions { MaxDegreeOfParallelism = 1 },
                new ParallelOptions { MaxDegreeOfParallelism = 1 },
                item =>
                {
                    if (sorted_replayData_grabbed[item] != null)
                    {
                        if (!sorted_replayData_grabbed[item].dupe)
                        {
                            if (sorted_replayData_grabbed[item].overallData != null)
                            {
                                if (sorted_replayData_grabbed[item].overallData.Mode != null)
                                {
                                    Console.WriteLine("Saving replay data for: " + item);
                                    if (sorted_replayData_grabbed[item].overallData.Mode != "Brawl")
                                    {
                                        sorted_replayData_grabbed[item].saveReplayData(sorted_replayData_grabbed[item].overallData);
                                    }
                                    else
                                    {
                                        sorted_replayData_grabbed[item].saveReplayDataBrawl(sorted_replayData_grabbed[item].overallData);

                                    }
                                }
                            }


                        }
                    }
                });

            /*
            foreach (var item in sorted_replayData_grabbed.Keys)
            {
                if (sorted_replayData_grabbed[item] != null)
                {
                    if (!sorted_replayData_grabbed[item].dupe)
                    {
                        if (sorted_replayData_grabbed[item].overallData != null)
                        {
                            if (sorted_replayData_grabbed[item].overallData.Mode != null)
                            {
                                Console.WriteLine("Saving replay data for: " + item);
                                if (sorted_replayData_grabbed[item].overallData.Mode != "Brawl")
                                {
                                    sorted_replayData_grabbed[item].saveReplayData(sorted_replayData_grabbed[item].overallData);
                                }
                                else
                                {
                                    sorted_replayData_grabbed[item].saveReplayDataBrawl(sorted_replayData_grabbed[item].overallData);

                                }
                            }
                        }
  
            
                    }
                }
            }

            */

        }
        private void recurseHotsApiCall(int maxValue)
        {
            try
            {
                string jsonString = "";
                Console.WriteLine("Grabbing Replay Data from HotsApi for replayID: " + maxValue);
                string url = @"https://hotsapi.net/api/v1/replays?min_id=" + maxValue;
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
                request.AutomaticDecompression = DecompressionMethods.GZip;

                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                using (Stream stream = response.GetResponseStream())
                using (StreamReader reader = new StreamReader(stream))
                {

                    jsonString = reader.ReadToEnd();

                }
                //jsonString = "[" + jsonString + ",{}]";
                Console.WriteLine(jsonString);
                HotsApiJSON.ReplayData[] data = HotsApiJSON.ReplayData.FromJson(jsonString);

                int counter = 0;
                for (int i = 0; i < data.Length; i++)
                {
                    //if (data[i].GameType != "Brawl")
                    //{
                    replays_to_run.Add(data[i].Id, data[i]);
                    counter++;

                    // }

                    if (counter == 100)
                    {
                        break;
                    }


                }
                int replays_left_counter = replays_to_run.Count;
                Parallel.ForEach(
                replays_to_run.Keys,
                //new ParallelOptions { MaxDegreeOfParallelism = -1 },
                //new ParallelOptions { MaxDegreeOfParallelism = 1 },
                new ParallelOptions { MaxDegreeOfParallelism = 100 },
                item =>
                {
                    replays_left_counter--;

                    Console.WriteLine("Running Reply: " + item + " - " + replays_left_counter + " left to run");
                    ParseStormReplay p = new ParseStormReplay(item, replays_to_run[item].Url, replays_to_run[item], maps, maps_translations, game_types, talents, seasons_game_versions, mmr_ids, seasons, heroes, heroes_translations, maps_short, mmr_ids, role, heroes_attr);
                    replayData_grabbed.TryAdd(item, p);
                });
                SortedDictionary<long, ParseStormReplay> sorted_replayData_grabbed = new SortedDictionary<long, ParseStormReplay>();

                foreach (var item in replayData_grabbed.Keys)
                {
                    sorted_replayData_grabbed.Add(item, replayData_grabbed[item]);
                }


                Parallel.ForEach(
                    sorted_replayData_grabbed.Keys,
                    //new ParallelOptions { MaxDegreeOfParallelism = -1 },
                    //new ParallelOptions { MaxDegreeOfParallelism = 1 },
                    new ParallelOptions { MaxDegreeOfParallelism = 1 },
                    item =>
                    {
                        if (sorted_replayData_grabbed[item] != null)
                        {
                            if (!sorted_replayData_grabbed[item].dupe)
                            {
                                if (sorted_replayData_grabbed[item].overallData != null)
                                {
                                    if (sorted_replayData_grabbed[item].overallData.Mode != null)
                                    {
                                        Console.WriteLine("Saving replay data for: " + item);
                                        if (sorted_replayData_grabbed[item].overallData.Mode != "Brawl")
                                        {
                                            sorted_replayData_grabbed[item].saveReplayData(sorted_replayData_grabbed[item].overallData);
                                        }
                                        else
                                        {
                                            sorted_replayData_grabbed[item].saveReplayDataBrawl(sorted_replayData_grabbed[item].overallData);

                                        }
                                    }
                                }
                            }
                        }

                    });
                /*
                foreach (var item in sorted_replayData_grabbed.Keys)
                {
                    if (sorted_replayData_grabbed[item] != null)
                    {
                        if (!sorted_replayData_grabbed[item].dupe)
                        {
                            if (sorted_replayData_grabbed[item].overallData != null)
                            {
                                if (sorted_replayData_grabbed[item].overallData.Mode != null)
                                {
                                    Console.WriteLine("Saving replay data for: " + item);
                                    if (sorted_replayData_grabbed[item].overallData.Mode != "Brawl")
                                    {
                                        sorted_replayData_grabbed[item].saveReplayData(sorted_replayData_grabbed[item].overallData);
                                    }
                                    else
                                    {
                                        sorted_replayData_grabbed[item].saveReplayDataBrawl(sorted_replayData_grabbed[item].overallData);

                                    }
                                }
                            }
                        }
                    }
                }
                */
            }
            catch (Exception e)
            {
                if (Regex.Match(e.ToString(), "Too Many Requests").Success)
                {
                    Console.WriteLine("Too many requests  - Sleeping for 10 seconds");
                    System.Threading.Thread.Sleep(10000);
                    recurseHotsApiCall(maxValue);
                }
            }

        }

    }
}
