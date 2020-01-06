using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using System.Net;
using System.IO;
using System.Text.RegularExpressions;
using HeroesProfile_Backend.Models;

namespace HeroesProfile_Backend
{
     public class GrabHotsApiData
    {
        private string _dbConnectString = new DB_Connect().heroesprofile_config;
        private Dictionary<string, string> _heroes = new Dictionary<string, string>();
        private Dictionary<string, string> _heroesTranslations = new Dictionary<string, string>();
        private Dictionary<string, string> _heroesAlt = new Dictionary<string, string>();
        private Dictionary<string, string> _heroesAttr = new Dictionary<string, string>();
        private Dictionary<string, string> _role = new Dictionary<string, string>();
        private Dictionary<string, string> _maps = new Dictionary<string, string>();
        private Dictionary<string, string> _mapsShort = new Dictionary<string, string>();
        private Dictionary<string, string> _mapsTranslations = new Dictionary<string, string>();
        private Dictionary<string, string> _gameTypes = new Dictionary<string, string>();
        private Dictionary<string, string> _talents = new Dictionary<string, string>();
        private Dictionary<string, string> _mmrIds = new Dictionary<string, string>();
        private Dictionary<string, string> _leagueTiers = new Dictionary<string, string>();
        private Dictionary<string, DateTime[]> _seasons = new Dictionary<string, DateTime[]>();
        private Dictionary<string, string> _seasonsGameVersions = new Dictionary<string, string>();
        private Dictionary<string, ReplaysNotProcessed> _notProcessedReplays = new Dictionary<string, ReplaysNotProcessed>();

        private Dictionary<long, HotsApiJSON.ReplayData> _replaysToRun = new Dictionary<long, HotsApiJSON.ReplayData>();



        private ConcurrentDictionary<long, ParseStormReplay> _replayDataGrabbed = new ConcurrentDictionary<long, ParseStormReplay>();

        public GrabHotsApiData()
        {
            var maxValue = 0;
            using var conn = new MySqlConnection(_dbConnectString);
            conn.Open();


            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT tier_id, name FROM league_tiers";
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    _leagueTiers.Add(reader.GetString("name"), reader.GetString("tier_id"));
                }
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT id, name, new_role FROM heroes";
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    _heroes.Add(reader.GetString("name"), reader.GetString("id"));
                    _role.Add(reader.GetString("name"), reader.GetString("new_role"));
                }
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT type_id, name FROM game_types";
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    _gameTypes.Add(reader.GetString("name").Replace(" ", ""), reader.GetString("type_id"));
                }
            }
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT * FROM heroesprofile.heroes_translations;";
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    if (!_heroesTranslations.ContainsKey(reader.GetString("translation").ToLower()))
                    {
                        _heroesTranslations.Add(reader.GetString("translation").ToLower(), reader.GetString("name"));

                    }
                }
            }
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT map_id, name, short_name FROM maps";
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    _maps.Add(reader.GetString("name"), reader.GetString("map_id"));
                    _mapsShort.Add(reader.GetString("short_name"), reader.GetString("name"));

                }
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT name, translation FROM maps_translations";
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    _mapsTranslations.Add(reader.GetString("translation"), reader.GetString("name"));
                }
            }




            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT hero_name, talent_id, talent_name FROM heroes_data_talents order by talent_id asc";
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    var hero = reader.GetString("hero_name");
                    var talent = reader.GetString("talent_name");
                    var split = Regex.Split(talent, @"(?<!^)(?=[A-Z])");

                    if (hero == "" && split.Length > 0)
                    {
                        hero = split[0];
                    }
                    if (!_talents.ContainsKey(reader.GetString("hero_name") + "|" + reader.GetString("talent_name")))
                    {
                        _talents.Add(reader.GetString("hero_name") + "|" + reader.GetString("talent_name"), reader.GetString("talent_id"));

                    }
                }
            }


            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT name, alt_name, short_name, attribute_id FROM heroes";
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    if (_heroesAlt.ContainsKey(reader.GetString("name"))) continue;
                    var hero = reader.GetString("name");
                    var alt = reader["alt_name"].Equals(DBNull.Value) ? string.Empty : reader.GetString("alt_name");

                    if (alt == "")
                    {
                        alt = reader.GetString("short_name");
                        alt = char.ToUpper(alt.First()) + alt.Substring(1).ToLower();

                    }
                    _heroesAlt.Add(alt, hero);

                    _heroesAttr.Add(reader.GetString("attribute_id"), reader.GetString("name"));
                }
            }




            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT mmr_type_id, name FROM mmr_type_ids";
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    _mmrIds.Add(reader.GetString("name"), reader.GetString("mmr_type_id"));
                }
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT id, start_date, end_date FROM season_dates;";
                var reader = cmd.ExecuteReader();


                while (reader.Read())
                {
                    var dates = new DateTime[2];

                    dates[0] = DateTime.Parse(reader.GetString("start_date"));
                    dates[1] = DateTime.Parse(reader.GetString("end_date"));


                    _seasons.Add(reader.GetString("id"), dates);
                }
            }

            using (var cmd = conn.CreateCommand())
            {

                cmd.CommandText = "SELECT * FROM replays_not_processed WHERE count_parsed < 3 ORDER BY replayID ASC LIMIT 100";
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    var replayId = reader["replayID"].Equals(DBNull.Value) ? string.Empty : reader.GetString("replayID");
                    if ((reader["replayID"].Equals(DBNull.Value) ? string.Empty : reader.GetString("replayID")) == "") continue;
                    var ron = new ReplaysNotProcessed
                    {
                            replayID = reader.GetString("replayID"),
                            region = reader.GetString("region"),
                            game_type = reader.GetString("game_type"),
                            game_length = reader.GetString("game_length"),
                            game_date = reader.GetString("game_date"),
                            game_map = reader.GetString("game_map"),
                            game_version = reader.GetString("game_version"),
                            size = reader["size"].Equals(DBNull.Value) ? string.Empty : reader.GetString("size"),
                            date_parsed = reader["date_parsed"].Equals(DBNull.Value) ? string.Empty : reader.GetString("date_parsed"),
                            count_parsed = reader.GetString("count_parsed"),
                            url = reader.GetString("url"),
                            failure_status = reader["failure_status"].Equals(DBNull.Value) ? string.Empty : reader.GetString("failure_status"),
                            processed = reader.GetString("processed")
                    };

                    _notProcessedReplays.Add(replayId, ron);

                }
            }


            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT season, game_version FROM season_game_versions";
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    if (!_seasonsGameVersions.ContainsKey(reader.GetString("game_version")))
                    {
                        _seasonsGameVersions.Add(reader.GetString("game_version"), reader.GetString("season"));

                    }
                }
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT MAX(replayID) as max_replayID FROM replay";
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    maxValue = Convert.ToInt32(reader.GetString("max_replayID"));
                }
            }
            RunNotProcessed();

            var notProcessedMaxValue = 0;

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT MAX(replayID) as max_replayID FROM replays_not_processed";
                var reader = cmd.ExecuteReader();

                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        var value = reader["max_replayID"].Equals(DBNull.Value) ? string.Empty : reader.GetString("max_replayID");
                        if (value != "")
                        {
                            notProcessedMaxValue = Convert.ToInt32(reader.GetString("max_replayID"));
                        }
                    }
                }
            }
            var brawlMaxValue = 0;
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT if(MAX(replayID) is null, 0, MAX(replayID)) as max_replayID FROM heroesprofile_brawl.replay";
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    brawlMaxValue = Convert.ToInt32(reader.GetString("max_replayID"));
                }
            }

            maxValue++;
            if (notProcessedMaxValue > maxValue)
            {
                maxValue = notProcessedMaxValue;

                if (brawlMaxValue > maxValue)
                {
                    maxValue = brawlMaxValue;
                }
            }
            else if (brawlMaxValue > maxValue)
            {
                maxValue = (brawlMaxValue + 1);
            }
            //maxValue = 15757424;
            RecurseHotsApiCall(maxValue);
        }
        private void RunNotProcessed()
        {
            Parallel.ForEach(
                _notProcessedReplays.Keys,
                //new ParallelOptions { MaxDegreeOfParallelism = -1 },
                //new ParallelOptions { MaxDegreeOfParallelism = 1 },
                new ParallelOptions { MaxDegreeOfParallelism = 100 },
                item =>
                {
                    //
                    Console.WriteLine("Running Reply: " + item);
                    var p = new ParseStormReplay(Convert.ToInt64(item), new Uri(_notProcessedReplays[item].url, UriKind.Absolute), _notProcessedReplays[item], _maps, _mapsTranslations, _gameTypes, _talents, _seasonsGameVersions, _mmrIds, _seasons, _heroes, _heroesTranslations, _mapsShort, _mmrIds, _role, _heroesAttr);
                    _replayDataGrabbed.TryAdd(Convert.ToInt64(item), p);

                });

            var sortedReplayDataGrabbed = new SortedDictionary<long, ParseStormReplay>();

            foreach (var item in _replayDataGrabbed.Keys)
            {
                sortedReplayDataGrabbed.Add(item, _replayDataGrabbed[item]);
            }

            Parallel.ForEach(
                sortedReplayDataGrabbed.Keys,
                //new ParallelOptions { MaxDegreeOfParallelism = -1 },
                //new ParallelOptions { MaxDegreeOfParallelism = 1 },
                new ParallelOptions { MaxDegreeOfParallelism = 1 },
                item =>
                {
                    if (sortedReplayDataGrabbed[item] == null) return;
                    if (sortedReplayDataGrabbed[item].dupe) return;
                    if (sortedReplayDataGrabbed[item].overallData == null) return;
                    if (sortedReplayDataGrabbed[item].overallData.Mode == null) return;
                    Console.WriteLine("Saving replay data for: " + item);
                    if (sortedReplayDataGrabbed[item].overallData.Mode != "Brawl")
                    {
                        sortedReplayDataGrabbed[item].saveReplayData(sortedReplayDataGrabbed[item].overallData);
                    }
                    else
                    {
                        sortedReplayDataGrabbed[item].saveReplayDataBrawl(sortedReplayDataGrabbed[item].overallData);

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
        private void RecurseHotsApiCall(int maxValue)
        {
            try
            {
                var jsonString = "";
                Console.WriteLine("Grabbing Replay Data from HotsApi for replayID: " + maxValue);
                var url = @"https://hotsapi.net/api/v1/replays?min_id=" + maxValue;
                var request = (HttpWebRequest)WebRequest.Create(url);
                request.AutomaticDecompression = DecompressionMethods.GZip;

                using (var response = (HttpWebResponse)request.GetResponse())
                {
                    using var stream = response.GetResponseStream();
                    using var reader = new StreamReader(stream);
                    jsonString = reader.ReadToEnd();
                }

                //jsonString = "[" + jsonString + ",{}]";
                Console.WriteLine(jsonString);
                var data = HotsApiJSON.ReplayData.FromJson(jsonString);

                var counter = 0;
                foreach (var t in data)
                {
                    //if (data[i].GameType != "Brawl")
                    //{
                    _replaysToRun.Add(t.Id, t);
                    counter++;

                    // }

                    if (counter == 100)
                    {
                        break;
                    }
                }
                var replaysLeftCounter = _replaysToRun.Count;
                Parallel.ForEach(
                _replaysToRun.Keys,
                //new ParallelOptions { MaxDegreeOfParallelism = -1 },
                //new ParallelOptions { MaxDegreeOfParallelism = 1 },
                new ParallelOptions { MaxDegreeOfParallelism = 100 },
                item =>
                {
                    replaysLeftCounter--;

                    Console.WriteLine("Running Reply: " + item + " - " + replaysLeftCounter + " left to run");
                    var p = new ParseStormReplay(item, _replaysToRun[item].Url, _replaysToRun[item], _maps, _mapsTranslations, _gameTypes, _talents, _seasonsGameVersions, _mmrIds, _seasons, _heroes, _heroesTranslations, _mapsShort, _mmrIds, _role, _heroesAttr);
                    _replayDataGrabbed.TryAdd(item, p);
                });
                var sortedReplayDataGrabbed = new SortedDictionary<long, ParseStormReplay>();

                foreach (var item in _replayDataGrabbed.Keys)
                {
                    sortedReplayDataGrabbed.Add(item, _replayDataGrabbed[item]);
                }


                Parallel.ForEach(
                    sortedReplayDataGrabbed.Keys,
                    //new ParallelOptions { MaxDegreeOfParallelism = -1 },
                    //new ParallelOptions { MaxDegreeOfParallelism = 1 },
                    new ParallelOptions { MaxDegreeOfParallelism = 1 },
                    item =>
                    {
                        if (sortedReplayDataGrabbed[item] == null) return;
                        if (sortedReplayDataGrabbed[item].dupe) return;
                        if (sortedReplayDataGrabbed[item].overallData == null) return;
                        if (sortedReplayDataGrabbed[item].overallData.Mode == null) return;
                        Console.WriteLine("Saving replay data for: " + item);
                        if (sortedReplayDataGrabbed[item].overallData.Mode != "Brawl")
                        {
                            sortedReplayDataGrabbed[item].saveReplayData(sortedReplayDataGrabbed[item].overallData);
                        }
                        else
                        {
                            sortedReplayDataGrabbed[item].saveReplayDataBrawl(sortedReplayDataGrabbed[item].overallData);

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
                    RecurseHotsApiCall(maxValue);
                }
            }

        }

    }
}
