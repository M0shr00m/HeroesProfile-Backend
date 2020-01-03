using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


using System.Net;
using System.Collections.Specialized;
using System.IO;

using System.Web.Script.Serialization;
using MySql.Data.MySqlClient;
using System.Text.RegularExpressions;

namespace HeroesProfile_Backend
{

    class ParseStormReplay
    {
        private string db_connect_string = new DB_Connect().heroesprofile_config;
        public LambdaReplayData data = new LambdaReplayData();

        private Dictionary<string, string> heroes = new Dictionary<string, string>();
        private Dictionary<string, string> heroes_alt = new Dictionary<string, string>();
        private Dictionary<string, string> role = new Dictionary<string, string>();
        private Dictionary<string, string> maps = new Dictionary<string, string>();
        private Dictionary<string, string> maps_translations = new Dictionary<string, string>();
        private Dictionary<string, string> game_types = new Dictionary<string, string>();
        private Dictionary<string, string> talents = new Dictionary<string, string>();
        private Dictionary<string, string> seasons_game_versions = new Dictionary<string, string>();
        private Dictionary<string, string> mmr_ids = new Dictionary<string, string>();
        private Dictionary<string, DateTime[]> seasons = new Dictionary<string, DateTime[]>();
        private Dictionary<string, string> heroes_translations = new Dictionary<string, string>();
        private Dictionary<string, string> maps_short = new Dictionary<string, string>();
        private Dictionary<string, string> heroes_attr = new Dictionary<string, string>();

        private long replayID;
        private Uri replayURL;

        public bool dupe = false;
        public LambdaJson.ReplayData overallData;

        public ParseStormReplay(long replayID, Uri replayURL, HotsApiJSON.ReplayData hotsapi_data, Dictionary<string, string> maps, Dictionary<string, string> maps_translations, Dictionary<string, string> game_types, Dictionary<string, string> talents, Dictionary<string, string> seasons_game_versions, Dictionary<string, string> mmr_ids, Dictionary<string, DateTime[]> seasons, Dictionary<string, string> heroes, Dictionary<string, string> heroes_translations, Dictionary<string, string> maps_short, Dictionary<string, string> mmrs, Dictionary<string, string> roles, Dictionary<string, string> heroes_attr)
        {
            this.replayID = replayID;
            this.replayURL = replayURL;
            this.maps = maps;
            this.maps_translations = maps_translations;
            this.game_types = game_types;
            this.talents = talents;
            this.seasons_game_versions = seasons_game_versions;
            this.seasons = seasons;
            this.heroes_translations = heroes_translations;
            this.maps_short = maps_short;
            this.heroes = heroes;
            this.mmr_ids = mmrs;
            role = roles;
            this.heroes_attr = heroes_attr;

            try
            {
                var globalJson = "";
                var httpWebRequest = (HttpWebRequest)WebRequest.Create("https://a73l75cbzg.execute-api.eu-west-1.amazonaws.com/default/parse-hots");

                httpWebRequest.Method = "POST";
                httpWebRequest.Timeout = 1000000;

                using (var streamWriter = new StreamWriter(httpWebRequest.GetRequestStream()))
                {
                    var json = new JavaScriptSerializer().Serialize(new
                    {
                        //input = "http://hotsapi.s3-website-eu-west-1.amazonaws.com/c5a49c21-d3d0-c8d9-c904-b3d09feea5e9.StormReplay",
                        input = replayURL,
                        access = "", //Need to pull from config file or ENV
                        secret = "" //Need to pull from config file or ENV
                    });

                    streamWriter.Write(json);
                }

                var httpResponse = (HttpWebResponse)httpWebRequest.GetResponse();
                var result = "";
                using (var streamReader = new StreamReader(httpResponse.GetResponseStream()))
                {
                    result = streamReader.ReadToEnd();
                    //Console.WriteLine(result);
                    globalJson = result;
                }


                if (Regex.Match(result, "Error parsing replay: UnexpectedResult").Success)
                {
                    using (var conn = new MySqlConnection(db_connect_string))
                    {
                        conn.Open();
                        using (var cmd = conn.CreateCommand())
                        {
                            cmd.CommandText = "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                                                replayID + "," +
                                                "\"" + "NULL" + "\"" + "," +
                                                "\"" + hotsapi_data.Region + "\"" + "," +
                                                "\"" + hotsapi_data.GameType + "\"" + "," +
                                                "\"" + hotsapi_data.GameLength + "\"" + "," +
                                                "\"" + hotsapi_data.GameDate.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                                "\"" + hotsapi_data.GameMap + "\"" + "," +
                                                "\"" + hotsapi_data.GameVersion + "\"" + "," +
                                                "\"" + hotsapi_data.Size + "\"" + "," +
                                                "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                                1 + "," +
                                                "\"" + replayURL + "\"" + "," +
                                                "\"" + hotsapi_data.Processed + "\"" + "," +
                                                "\"" + "Error parsing replay: UnexpectedResult" + "\"" + ")";
                            var Reader = cmd.ExecuteReader();
                        }
                    }
                }
                else if (Regex.Match(result, "Error parsing replay: SuccessReplayDetail").Success)
                {
                    using (var conn = new MySqlConnection(db_connect_string))
                    {
                        conn.Open();
                        using (var cmd = conn.CreateCommand())
                        {
                            cmd.CommandText = "INSERT IGNORE INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                                                replayID + "," +
                                                "\"" + "NULL" + "\"" + "," +
                                                "\"" + hotsapi_data.Region + "\"" + "," +
                                                "\"" + hotsapi_data.GameType + "\"" + "," +
                                                "\"" + hotsapi_data.GameLength + "\"" + "," +
                                                "\"" + hotsapi_data.GameDate.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                                "\"" + hotsapi_data.GameMap + "\"" + "," +
                                                "\"" + hotsapi_data.GameVersion + "\"" + "," +
                                                "\"" + hotsapi_data.Size + "\"" + "," +
                                                "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                                1 + "," +
                                                "\"" + replayURL + "\"" + "," +
                                                "\"" + hotsapi_data.Processed + "\"" + "," +
                                                "\"" + "Error parsing replay: SuccessReplayDetail" + "\"" + ")";
                            var Reader = cmd.ExecuteReader();
                        }
                    }
                }
                else if (Regex.Match(result, "Error parsing replay: ParserException").Success)
                {
                    using (var conn = new MySqlConnection(db_connect_string))
                    {
                        conn.Open();
                        using (var cmd = conn.CreateCommand())
                        {
                            cmd.CommandText = "INSERT IGNORE INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                                                replayID + "," +
                                                "\"" + "NULL" + "\"" + "," +
                                                "\"" + hotsapi_data.Region + "\"" + "," +
                                                "\"" + hotsapi_data.GameType + "\"" + "," +
                                                "\"" + hotsapi_data.GameLength + "\"" + "," +
                                                "\"" + hotsapi_data.GameDate.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                                "\"" + hotsapi_data.GameMap + "\"" + "," +
                                                "\"" + hotsapi_data.GameVersion + "\"" + "," +
                                                "\"" + hotsapi_data.Size + "\"" + "," +
                                                "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                                1 + "," +
                                                "\"" + replayURL + "\"" + "," +
                                                "\"" + hotsapi_data.Processed + "\"" + "," +
                                                "\"" + "Error parsing replay: ParserException" + "\"" + ")";
                            var Reader = cmd.ExecuteReader();
                        }
                    }
                }
                else
                {
                    var data = LambdaJson.ReplayData.FromJson(globalJson);
                    overallData = data;
                    //if (data.Mode != "Brawl")
                    //{
                    if (data.Version != null)
                    {
                        var version = new Version(data.Version);
                        data.VersionSplit = version.Minor + "." + version.Build + "." + version.Revision + "." + data.VersionBuild;




                        for (var h = 0; h < data.Players.Length; h++)
                        {
                            if (heroes_translations.ContainsKey(data.Players[h].Hero.ToLower()))
                            {
                                data.Players[h].Hero = heroes_translations[data.Players[h].Hero.ToLower()];
                            }

                            DateTimeOffset dateValue;
                            if (DateTimeOffset.TryParse(data.Players[h].Score.TimeCCdEnemyHeroes, out dateValue))
                            {
                                data.Players[h].Score.TimeCCdEnemyHeroes_not_null = dateValue;

                            }
                            else
                            {
                                data.Players[h].Score.TimeCCdEnemyHeroes_not_null = DateTimeOffset.Parse("00:00:00");

                            }

                        }

                        if (maps_translations.ContainsKey(data.Map))
                        {
                            data.Map = maps_translations[data.Map];
                        }
                        else
                        {
                            if (maps_short.ContainsKey(data.MapShort))
                            {
                                data.Map = maps_short[data.MapShort];
                            }
                        }

                        //if (data.Mode != "Brawl" && data.Mode != null && data.Date != null && data.Length != null && data.Region != null)
                        if (data.Mode != null && data.Date != null && data.Length != null && data.Region != null)
                        {


                            var orderedPlayers = new LambdaJson.Player[10];

                            var team1 = 0;
                            var team2 = 5;
                            for (var j = 0; j < data.Players.Length; j++)
                            {
                                if (data.Players[j].Team == 0)
                                {
                                    orderedPlayers[team1] = data.Players[j];
                                    team1++;
                                }
                                else if (data.Players[j].Team == 1)
                                {
                                    orderedPlayers[team2] = data.Players[j];
                                    team2++;
                                }
                            }
                            data.Players = orderedPlayers;

                            var badMap = false;
                            var badGameType = false;
                            if (maps.ContainsKey(data.Map))
                            {
                                data.GameMap_id = maps[data.Map];

                            }
                            else
                            {
                                badMap = true;
                            }

                            if (game_types.ContainsKey(data.Mode))
                            {
                                data.GameType_id = game_types[data.Mode];

                            }
                            else
                            {
                                badGameType = true;
                            }

                            if (!badMap && !badGameType)
                            {

                                using (var conn = new MySqlConnection(db_connect_string))
                                {
                                    conn.Open();
                                    using (var cmd = conn.CreateCommand())
                                    {
                                        if (data.Mode != "Brawl")
                                        {
                                            cmd.CommandText = "SELECT replayID FROM replay where replayID = " + replayID;
                                        }
                                        else
                                        {
                                            cmd.CommandText = "SELECT replayID FROM heroesprofile_brawl.replay where replayID = " + replayID;
                                        }
                                        var Reader = cmd.ExecuteReader();

                                        if (Reader.HasRows)
                                        {
                                            dupe = true;
                                        }
                                    }
                                }
                            }
                            else
                            {
                                using (var conn = new MySqlConnection(db_connect_string))
                                {
                                    conn.Open();
                                    using (var cmd = conn.CreateCommand())
                                    {
                                        cmd.CommandText = "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                                                            replayID + "," +
                                                            "\"" + "NULL" + "\"" + "," +
                                                            "\"" + hotsapi_data.Region + "\"" + "," +
                                                            "\"" + hotsapi_data.GameType + "\"" + "," +
                                                            "\"" + hotsapi_data.GameLength + "\"" + "," +
                                                            "\"" + hotsapi_data.GameDate.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                                            "\"" + hotsapi_data.GameMap + "\"" + "," +
                                                            "\"" + hotsapi_data.GameVersion + "\"" + "," +
                                                            "\"" + hotsapi_data.Size + "\"" + "," +
                                                            "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                                            1 + "," +
                                                            "\"" + replayURL + "\"" + "," +
                                                            "\"" + hotsapi_data.Processed + "\"" + "," +
                                                            "\"" + "Map or Game Type Bad" + "\"" + ")";
                                        cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                                                            "replayId = VALUES(replayId), " +
                                                            "parsedID = VALUES(parsedID)," +
                                                            "region = VALUES(region)," +
                                                            "game_type = VALUES(game_type)," +
                                                            "game_length = VALUES(game_length)," +
                                                            "game_date = VALUES(game_date)," +
                                                            "game_map = VALUES(game_map)," +
                                                            "game_version = VALUES(game_version)," +
                                                            "size = VALUES(size)," +
                                                            "date_parsed = VALUES(date_parsed)," +
                                                            "count_parsed = count_parsed + VALUES(count_parsed), " +
                                                            "url = VALUES(url)," +
                                                            "processed = VALUES(processed)," +
                                                            "failure_status = VALUES(failure_status)";
                                        cmd.CommandTimeout = 0;
                                        //Console.WriteLine(cmd.CommandText);
                                        var Reader = cmd.ExecuteReader();
                                    }
                                }

                            }

                        }
                        else
                        {
                            using (var conn = new MySqlConnection(db_connect_string))
                            {
                                conn.Open();
                                using (var cmd = conn.CreateCommand())
                                {
                                    cmd.CommandText = "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                                                        replayID + "," +
                                                        "\"" + "NULL" + "\"" + "," +
                                                        "\"" + hotsapi_data.Region + "\"" + "," +
                                                        "\"" + hotsapi_data.GameType + "\"" + "," +
                                                        "\"" + hotsapi_data.GameLength + "\"" + "," +
                                                        "\"" + hotsapi_data.GameDate.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                                        "\"" + hotsapi_data.GameMap + "\"" + "," +
                                                        "\"" + hotsapi_data.GameVersion + "\"" + "," +
                                                        "\"" + hotsapi_data.Size + "\"" + "," +
                                                        "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                                        1 + "," +
                                                        "\"" + replayURL + "\"" + "," +
                                                        "\"" + hotsapi_data.Processed + "\"" + "," +
                                                        "\"" + "Map or Game Type Bad" + "\"" + ")";
                                    cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                                                        "replayId = VALUES(replayId), " +
                                                        "parsedID = VALUES(parsedID)," +
                                                        "region = VALUES(region)," +
                                                        "game_type = VALUES(game_type)," +
                                                        "game_length = VALUES(game_length)," +
                                                        "game_date = VALUES(game_date)," +
                                                        "game_map = VALUES(game_map)," +
                                                        "game_version = VALUES(game_version)," +
                                                        "size = VALUES(size)," +
                                                        "date_parsed = VALUES(date_parsed)," +
                                                        "count_parsed = count_parsed + VALUES(count_parsed), " +
                                                        "url = VALUES(url)," +
                                                        "processed = VALUES(processed)," +
                                                        "failure_status = VALUES(failure_status)";
                                    cmd.CommandTimeout = 0;
                                    //Console.WriteLine(cmd.CommandText);
                                    var Reader = cmd.ExecuteReader();
                                }
                            }
                        }
                    }
                    else
                    {
                        using (var conn = new MySqlConnection(db_connect_string))
                        {
                            conn.Open();
                            using (var cmd = conn.CreateCommand())
                            {
                                cmd.CommandText = "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                                                    replayID + "," +
                                                    "\"" + "NULL" + "\"" + "," +
                                                    "\"" + hotsapi_data.Region + "\"" + "," +
                                                    "\"" + hotsapi_data.GameType + "\"" + "," +
                                                    "\"" + hotsapi_data.GameLength + "\"" + "," +
                                                    "\"" + hotsapi_data.GameDate.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                                    "\"" + hotsapi_data.GameMap + "\"" + "," +
                                                    "\"" + "" + "\"" + "," +
                                                    "\"" + hotsapi_data.Size + "\"" + "," +
                                                    "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                                    1 + "," +
                                                    "\"" + replayURL + "\"" + "," +
                                                    "\"" + hotsapi_data.Processed + "\"" + "," +
                                                    "\"" + "Game Version Null" + "\"" + ")";
                                cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                                                    "replayId = VALUES(replayId), " +
                                                    "parsedID = VALUES(parsedID)," +
                                                    "region = VALUES(region)," +
                                                    "game_type = VALUES(game_type)," +
                                                    "game_length = VALUES(game_length)," +
                                                    "game_date = VALUES(game_date)," +
                                                    "game_map = VALUES(game_map)," +
                                                    "game_version = VALUES(game_version)," +
                                                    "size = VALUES(size)," +
                                                    "date_parsed = VALUES(date_parsed)," +
                                                    "count_parsed = count_parsed + VALUES(count_parsed), " +
                                                    "url = VALUES(url)," +
                                                    "processed = VALUES(processed)," +
                                                    "failure_status = VALUES(failure_status)";
                                cmd.CommandTimeout = 0;
                                //Console.WriteLine(cmd.CommandText);
                                var Reader = cmd.ExecuteReader();
                            }
                        }
                    }


                    //}

                }
            }
            catch (Exception e)
            {
                using (var conn = new MySqlConnection(db_connect_string))
                {
                    conn.Open();
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "INSERT IGNORE INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                                            replayID + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + 0 + "\"" + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + hotsapi_data.GameDate.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                            1 + "," +
                                            "\"" + replayURL + "\"" + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + e.ToString() + "\"" + ")";
                        var Reader = cmd.ExecuteReader();
                    }
                }
            }

        }

        public ParseStormReplay(long replayID, Uri replayURL, ReplaysNotProcessed notProcessedReplays, Dictionary<string, string> maps, Dictionary<string, string> maps_translations, Dictionary<string, string> game_types, Dictionary<string, string> talents, Dictionary<string, string> seasons_game_versions, Dictionary<string, string> mmr_ids, Dictionary<string, DateTime[]> seasons, Dictionary<string, string> heroes, Dictionary<string, string> heroes_translations, Dictionary<string, string> maps_short, Dictionary<string, string> mmrs, Dictionary<string, string> roles, Dictionary<string, string> heroes_attr)
        {
            this.replayID = replayID;
            this.replayURL = replayURL;
            this.maps = maps;
            this.maps_translations = maps_translations;
            this.game_types = game_types;
            this.talents = talents;
            this.seasons_game_versions = seasons_game_versions;
            this.seasons = seasons;
            this.heroes_translations = heroes_translations;
            this.maps_short = maps_short;
            this.heroes = heroes;
            this.mmr_ids = mmrs;
            role = roles;
            this.heroes_attr = heroes_attr;

            try
            {
                var globalJson = "";
                var httpWebRequest = (HttpWebRequest)WebRequest.Create("https://a73l75cbzg.execute-api.eu-west-1.amazonaws.com/default/parse-hots");

                httpWebRequest.Method = "POST";
                httpWebRequest.Timeout = 1000000;

                using (var streamWriter = new StreamWriter(httpWebRequest.GetRequestStream()))
                {
                    var json = new JavaScriptSerializer().Serialize(new
                    {
                        //input = "http://hotsapi.s3-website-eu-west-1.amazonaws.com/c5a49c21-d3d0-c8d9-c904-b3d09feea5e9.StormReplay",
                        input = replayURL,
                        access = "", //Need to pull from config file or ENV
                        secret = "" //Need to pull from config file or ENV
                    });

                    streamWriter.Write(json);
                }

                var httpResponse = (HttpWebResponse)httpWebRequest.GetResponse();
                var result = "";
                using (var streamReader = new StreamReader(httpResponse.GetResponseStream()))
                {
                    result = streamReader.ReadToEnd();
                    //Console.WriteLine(result);
                    globalJson = result;
                }


                if (Regex.Match(result, "Error parsing replay: UnexpectedResult").Success)
                {
                    using (var conn = new MySqlConnection(db_connect_string))
                    {
                        conn.Open();
                        using (var cmd = conn.CreateCommand())
                        {
                            cmd.CommandText = "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                                           replayID + "," +
                                           "\"" + "NULL" + "\"" + "," +
                                           "\"" + 0 + "\"" + "," +
                                           "\"" + "NULL" + "\"" + "," +
                                           "\"" + "NULL" + "\"" + "," +
                                           "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                               "\"" + "NULL" + "\"" + "," +
                                                "\"" + "NULL" + "\"" + "," +
                                                "\"" + "NULL" + "\"" + "," +
                                                "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                                1 + "," +
                                                "\"" + replayURL + "\"" + "," +
                                                "\"" + "NULL" + "\"" + "," +
                                                "\"" + "Error parsing replay: UnexpectedResult" + "\"" + ")";
                            cmd.CommandText += " ON DUPLICATE KEY UPDATE count_parsed = count_parsed + 1";
                            var Reader = cmd.ExecuteReader();
                        }
                    }
                }

                else if (Regex.Match(result, "Error parsing replay: SuccessReplayDetail").Success)
                {
                    using (var conn = new MySqlConnection(db_connect_string))
                    {
                        conn.Open();
                        using (var cmd = conn.CreateCommand())
                        {
                            cmd.CommandText = "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                                           replayID + "," +
                                           "\"" + "NULL" + "\"" + "," +
                                           "\"" + 0 + "\"" + "," +
                                           "\"" + "NULL" + "\"" + "," +
                                           "\"" + "NULL" + "\"" + "," +
                                           "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                               "\"" + "NULL" + "\"" + "," +
                                                "\"" + "NULL" + "\"" + "," +
                                                "\"" + "NULL" + "\"" + "," +
                                                "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                                1 + "," +
                                                "\"" + replayURL + "\"" + "," +
                                                "\"" + "NULL" + "\"" + "," +
                                                "\"" + "Error parsing replay: SuccessReplayDetail" + "\"" + ")";
                            cmd.CommandText += " ON DUPLICATE KEY UPDATE count_parsed = count_parsed + 1";
                            var Reader = cmd.ExecuteReader();
                        }
                    }
                }
                else if (Regex.Match(result, "Error parsing replay: ParserException").Success)
                {
                    using (var conn = new MySqlConnection(db_connect_string))
                    {
                        conn.Open();
                        using (var cmd = conn.CreateCommand())
                        {
                            cmd.CommandText = "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                                           replayID + "," +
                                           "\"" + "NULL" + "\"" + "," +
                                           "\"" + 0 + "\"" + "," +
                                           "\"" + "NULL" + "\"" + "," +
                                           "\"" + "NULL" + "\"" + "," +
                                           "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                               "\"" + "NULL" + "\"" + "," +
                                                "\"" + "NULL" + "\"" + "," +
                                                "\"" + "NULL" + "\"" + "," +
                                                "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                                1 + "," +
                                                "\"" + replayURL + "\"" + "," +
                                                "\"" + "NULL" + "\"" + "," +
                                                "\"" + "Error parsing replay: ParserException" + "\"" + ")";
                            cmd.CommandText += " ON DUPLICATE KEY UPDATE count_parsed = count_parsed + 1";
                            var Reader = cmd.ExecuteReader();
                        }
                    }
                }

                else
                {
                    var data = LambdaJson.ReplayData.FromJson(globalJson);
                    overallData = data;
                    //if (data.Mode != "Brawl")
                    // {
                    if (data.Version != null)
                    {
                        var version = new Version(data.Version);
                        data.VersionSplit = version.Minor + "." + version.Build + "." + version.Revision + "." + data.VersionBuild;




                        for (var h = 0; h < data.Players.Length; h++)
                        {
                            if (heroes_translations.ContainsKey(data.Players[h].Hero.ToLower()))
                            {
                                data.Players[h].Hero = heroes_translations[data.Players[h].Hero.ToLower()];
                            }

                            DateTimeOffset dateValue;
                            if (DateTimeOffset.TryParse(data.Players[h].Score.TimeCCdEnemyHeroes, out dateValue))
                            {
                                data.Players[h].Score.TimeCCdEnemyHeroes_not_null = dateValue;

                            }
                            else
                            {
                                data.Players[h].Score.TimeCCdEnemyHeroes_not_null = DateTimeOffset.Parse("00:00:00");

                            }

                        }
                        if (maps_translations.ContainsKey(data.Map))
                        {
                            data.Map = maps_translations[data.Map];
                        }
                        else
                        {
                            if (maps_short.ContainsKey(data.MapShort))
                            {
                                data.Map = maps_short[data.MapShort];
                            }
                        }



                        // if (data.Mode != "Brawl" && data.Mode != null && data.Date != null && data.Length != null && data.Region != null)
                        if (data.Mode != null && data.Date != null && data.Length != null && data.Region != null)
                        {


                            var orderedPlayers = new LambdaJson.Player[10];

                            var team1 = 0;
                            var team2 = 5;
                            for (var j = 0; j < data.Players.Length; j++)
                            {
                                if (data.Players[j].Team == 0)
                                {
                                    orderedPlayers[team1] = data.Players[j];
                                    team1++;
                                }
                                else if (data.Players[j].Team == 1)
                                {
                                    orderedPlayers[team2] = data.Players[j];
                                    team2++;
                                }
                            }
                            data.Players = orderedPlayers;

                            var badMap = false;
                            var badGameType = false;
                            if (maps.ContainsKey(data.Map))
                            {
                                data.GameMap_id = maps[data.Map];

                            }
                            else
                            {
                                badMap = true;
                            }

                            if (game_types.ContainsKey(data.Mode))
                            {
                                data.GameType_id = game_types[data.Mode];

                            }
                            else
                            {
                                badGameType = true;
                            }

                            if (!badMap && !badGameType)
                            {
                                using (var conn = new MySqlConnection(db_connect_string))
                                {
                                    conn.Open();
                                    using (var cmd = conn.CreateCommand())
                                    {
                                        if (data.Mode != "Brawl")
                                        {
                                            cmd.CommandText = "SELECT replayID FROM replay where replayID = " + replayID;
                                        }
                                        else
                                        {
                                            cmd.CommandText = "SELECT replayID FROM heroesprofile_brawl.replay where replayID = " + replayID;
                                        }
                                        var Reader = cmd.ExecuteReader();

                                        if (Reader.HasRows)
                                        {
                                            dupe = true;
                                        }
                                    }
                                }

                            }
                            else
                            {
                                using (var conn = new MySqlConnection(db_connect_string))
                                {
                                    conn.Open();
                                    using (var cmd = conn.CreateCommand())
                                    {
                                        cmd.CommandText = "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                                                            replayID + "," +
                                                            "\"" + "NULL" + "\"" + "," +
                                                            "\"" + 0 + "\"" + "," +
                                                            "\"" + "NULL" + "\"" + "," +
                                                            "\"" + "NULL" + "\"" + "," +
                                                            "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                                            "\"" + "NULL" + "\"" + "," +
                                                            "\"" + "NULL" + "\"" + "," +
                                                            "\"" + "NULL" + "\"" + "," +
                                                            "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                                            1 + "," +
                                                            "\"" + replayURL + "\"" + "," +
                                                            "\"" + "NULL" + "\"" + "," +
                                                            "\"" + "Map or Game Type Bad" + "\"" + ")";
                                        cmd.CommandText += " ON DUPLICATE KEY UPDATE count_parsed = count_parsed + 1";
                                        var Reader = cmd.ExecuteReader();
                                    }
                                }

                            }

                        }
                        else
                        {
                            using (var conn = new MySqlConnection(db_connect_string))
                            {
                                conn.Open();
                                using (var cmd = conn.CreateCommand())
                                {
                                    cmd.CommandText = "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                                                        replayID + "," +
                                                        "\"" + "NULL" + "\"" + "," +
                                                        "\"" + 0 + "\"" + "," +
                                                        "\"" + "NULL" + "\"" + "," +
                                                        "\"" + "NULL" + "\"" + "," +
                                                        "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                                        "\"" + "NULL" + "\"" + "," +
                                                        "\"" + "NULL" + "\"" + "," +
                                                        "\"" + "NULL" + "\"" + "," +
                                                        "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                                        1 + "," +
                                                        "\"" + replayURL + "\"" + "," +
                                                        "\"" + "NULL" + "\"" + "," +
                                                        "\"" + "Map or Game Type Bad" + "\"" + ")";
                                    cmd.CommandText += " ON DUPLICATE KEY UPDATE count_parsed = count_parsed + 1";
                                    var Reader = cmd.ExecuteReader();
                                }
                            }
                        }
                    }
                    else
                    {
                        using (var conn = new MySqlConnection(db_connect_string))
                        {
                            conn.Open();
                            using (var cmd = conn.CreateCommand())
                            {
                                cmd.CommandText = "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                                                    replayID + "," +
                                                    "\"" + "NULL" + "\"" + "," +
                                                    "\"" + 0 + "\"" + "," +
                                                    "\"" + "NULL" + "\"" + "," +
                                                    "\"" + "NULL" + "\"" + "," +
                                                    "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                                    "\"" + "NULL" + "\"" + "," +
                                                    "\"" + "NULL" + "\"" + "," +
                                                    "\"" + "NULL" + "\"" + "," +
                                                    "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                                    1 + "," +
                                                    "\"" + replayURL + "\"" + "," +
                                                    "\"" + "NULL" + "\"" + "," +
                                                    "\"" + "Game Version Null" + "\"" + ")";
                                cmd.CommandText += " ON DUPLICATE KEY UPDATE count_parsed = count_parsed + 1";
                                var Reader = cmd.ExecuteReader();
                            }
                        }
                    }


                    //}

                }

                if (dupe)
                {
                    using (var conn = new MySqlConnection(db_connect_string))
                    {
                        conn.Open();
                        using (var cmd = conn.CreateCommand())
                        {
                            cmd.CommandText = "DELETE FROM replays_not_processed WHERE replayID = " + replayID;
                            var Reader = cmd.ExecuteReader();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                using (var conn = new MySqlConnection(db_connect_string))
                {
                    conn.Open();
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                                            replayID + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + 0 + "\"" + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                            1 + "," +
                                            "\"" + replayURL + "\"" + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + e.ToString() + "\"" + ")";
                        cmd.CommandText += " ON DUPLICATE KEY UPDATE count_parsed = count_parsed + 1";
                        var Reader = cmd.ExecuteReader();
                    }
                }
            }

        }

        public void saveReplayData(LambdaJson.ReplayData data)
        {
            try
            {
                if (data.Players != null)
                {
                    using (var conn = new MySqlConnection(db_connect_string))
                    {
                        conn.Open();
                        var badHeroName = false;
                        var badTalentName = false;
                        if (data.Players.Length == 10)
                        {
                            for (var i = 0; i < data.Players.Length; i++)
                            {
                                if (data.Players[i].Hero == null)
                                {
                                    if (data.Players[i].Talents != null)
                                    {
                                        var split = Regex.Split(data.Players[i].Talents[0], @"(?<!^)(?=[A-Z])");

                                        if (heroes.ContainsKey(split[0]))
                                        {
                                            data.Players[i].Hero = split[0];
                                            data.Players[i].Hero_id = heroes[split[0]];
                                        }
                                        else
                                        {
                                            badHeroName = true;
                                            break;

                                        }
                                    }
                                    else
                                    {
                                        badHeroName = true;
                                        break;
                                    }


                                }
                                else
                                {
                                    if (!heroes.ContainsKey(data.Players[i].Hero))
                                    {
                                        Console.WriteLine(data.Players[i].Hero);
                                        Console.WriteLine(data.Players[i].Talents[0]);
                                        badHeroName = true;
                                        break;
                                    }
                                    else
                                    {
                                        data.Players[i].Hero_id = heroes[data.Players[i].Hero];
                                    }
                                }


                                if (data.Players[i].Talents != null)
                                {
                                    for (var t = 0; t < data.Players[i].Talents.Length; t++)
                                    {
                                        if (!talents.ContainsKey(data.Players[i].Hero + "|" + data.Players[i].Talents[t]))
                                        {
                                            talents.Add(data.Players[i].Hero + "|" + data.Players[i].Talents[t], insertIntoTalentTable(data.Players[i].Hero, data.Players[i].Talents[t], conn));
                                        }
                                    }

                                }
                            }
                            var team_one_level_ten_time = DateTimeOffset.Now;
                            var team_two_level_ten_time = DateTimeOffset.Now;

                            for (var teams = 0; teams < data.TeamExperience.Length; teams++)
                            {
                                for (var team_time_split = 0; team_time_split < data.TeamExperience[teams].Length; team_time_split++)
                                {
                                    if (data.TeamExperience[teams][team_time_split].TeamLevel >= 10)
                                    {
                                        if (teams == 0)
                                        {
                                            team_one_level_ten_time = data.TeamExperience[teams][team_time_split].TimeSpan;
                                            break;

                                        }
                                        else
                                        {
                                            team_two_level_ten_time = data.TeamExperience[teams][team_time_split].TimeSpan;
                                            break;

                                        }
                                    }
                                }
                            }
                            var team_one_first_to_ten = 0;
                            var team_two_first_to_ten = 0;
                            if (team_one_level_ten_time < team_two_level_ten_time)
                            {
                                team_one_first_to_ten = 1;
                            }
                            else
                            {
                                team_two_first_to_ten = 1;
                            }

                            for (var i = 0; i < data.Players.Length; i++)
                            {
                                if (data.Players[i].Team == 0)
                                {
                                    data.Players[i].Score.FirstToTen = team_one_first_to_ten;
                                }
                                else
                                {
                                    data.Players[i].Score.FirstToTen = team_two_first_to_ten;

                                }
                            }


                            if (!badHeroName)
                            {

                                for (var i = 0; i < data.Players.Length; i++)
                                {

                                    //Console.WriteLine("Adding Battletag to battletag table for: " + data.Players.Battletag);

                                    using (var cmd = conn.CreateCommand())
                                    {
                                        cmd.CommandText = "INSERT INTO battletags (blizz_id, battletag, region, account_level, latest_game) VALUES " +
                                            "(" + data.Players[i].BlizzId + "," +
                                            "\"" + data.Players[i].BattletagName + "#" + data.Players[i].BattletagId + "\"" + "," +
                                            data.Region + "," +
                                            data.Players[i].AccountLevel + "," +
                                            "\"" + data.Date.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + ")";

                                        cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                                                            "blizz_id = VALUES(blizz_id), " +
                                                            "battletag = VALUES(battletag), " +
                                                            "region = VALUES(region), " +
                                                            "account_level = IF(VALUES(account_level) > account_level, VALUES(account_level), account_level), " +
                                                            "latest_game = IF(VALUES(latest_game) > latest_game, VALUES(latest_game), latest_game) ";
                                        cmd.CommandTimeout = 0;



                                        var Reader = cmd.ExecuteReader();
                                    }
                                    //Console.WriteLine("Getting player_id for: " + data.Players.Battletag);


                                    using (var cmd = conn.CreateCommand())
                                    {

                                        cmd.CommandText = "SELECT player_id from battletags where battletag = " + "\"" + data.Players[i].BattletagName + "#" + data.Players[i].BattletagId + "\"";
                                        cmd.CommandTimeout = 0;
                                        var Reader = cmd.ExecuteReader();

                                        while (Reader.Read())
                                        {
                                            data.Players[i].battletag_table_id = Reader.GetString("player_id");
                                        }
                                    }
                                }



                                //Console.WriteLine("Saving Replay Information for:" + data.Id);

                                using (var cmd = conn.CreateCommand())
                                {
                                    cmd.CommandText = "INSERT INTO replay (replayID, parsed_id, game_type, game_date, game_length, game_map, game_version, region, date_added) VALUES(" +
                                        replayID + "," +
                                        "NULL" + "," +
                                        game_types[data.Mode] + "," +
                                        "\"" + data.Date.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                        data.Length.UtcDateTime.TimeOfDay.TotalSeconds + "," +
                                        maps[data.Map] + "," +
                                        "\"" + data.VersionSplit + "\"" + "," +
                                        data.Region + "," +
                                        "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + ")";
                                    cmd.CommandTimeout = 0;
                                    //Console.WriteLine(cmd.CommandText);
                                    var Reader = cmd.ExecuteReader();
                                }


                                using (var cmd = conn.CreateCommand())
                                {
                                    cmd.CommandText = "DELETE FROM replays_not_processed WHERE replayID = " + replayID;
                                    var Reader = cmd.ExecuteReader();
                                }


                                if (game_types[data.Mode] == "5")
                                {
                                    using (var cmd = conn.CreateCommand())
                                    {
                                        cmd.CommandText = "INSERT IGNORE INTO replays_ran_mmr (replayID, game_date) VALUES(" +
                                            replayID + "," +

                                            "\"" + data.Date.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + ")";
                                        cmd.CommandTimeout = 0;
                                        // Console.WriteLine(cmd.CommandText);
                                        var Reader = cmd.ExecuteReader();
                                    }
                                }


                                for (var i = 0; i < data.Bans.Length; i++)
                                {
                                    for (var j = 0; j < data.Bans[i].Length; j++)
                                    {
                                        if (data.Bans[i][j] != null)
                                        {
                                            if (heroes_attr.ContainsKey(data.Bans[i][j].ToString()))
                                            {
                                                data.Bans[i][j] = heroes_attr[data.Bans[i][j].ToString()];

                                            }
                                        }

                                    }
                                }
                                if (data.Bans != null)
                                {
                                    for (var i = 0; i < data.Bans.Length; i++)
                                    {
                                        for (var j = 0; j < data.Bans[i].Length; j++)
                                        {
                                            using (var cmd = conn.CreateCommand())
                                            {
                                                var value = "0";

                                                if (data.Bans[i][j] != null)
                                                {
                                                    value = heroes[data.Bans[i][j].ToString()];
                                                }
                                                cmd.CommandText = "INSERT INTO replay_bans (replayID, team, hero) VALUES(" +
                                                    replayID + "," +
                                                    i + "," +
                                                    value + ")";
                                                cmd.CommandTimeout = 0;
                                                //Console.WriteLine(cmd.CommandText);
                                                var Reader = cmd.ExecuteReader();
                                            }
                                        }
                                    }
                                }


                                for (var i = 0; i < data.TeamExperience.Length; i++)
                                {
                                    for (var j = 0; j < data.TeamExperience[i].Length; j++)
                                    {
                                        using (var cmd = conn.CreateCommand())
                                        {
                                            cmd.CommandText = "INSERT into replay_experience_breakdown (replayID, team, team_level, timestamp, structureXP, creepXP, heroXP, minionXP, trickXP, totalXP) VALUES(" +
                                            "\"" + replayID + "\"" + "," +
                                            "\"" + i + "\"" + "," +
                                            "\"" + data.TeamExperience[i][j].TeamLevel + "\"" + "," +
                                            "\"" + data.TeamExperience[i][j].TimeSpan.ToString("HH:mm:ss") + "\"" + "," +
                                            "\"" + data.TeamExperience[i][j].StructureXp + "\"" + "," +
                                            "\"" + data.TeamExperience[i][j].CreepXp + "\"" + "," +
                                            "\"" + data.TeamExperience[i][j].HeroXp + "\"" + "," +
                                            "\"" + data.TeamExperience[i][j].MinionXp + "\"" + "," +
                                            "\"" + data.TeamExperience[i][j].TrickleXp + "\"" + "," +
                                            "\"" + data.TeamExperience[i][j].TotalXp + "\"" + ")";
                                            cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                                                    "replayID = VALUES(replayID), " +
                                                    "team = VALUES(team), " +
                                                    "team_level = VALUES(team_level)," +
                                                    "timestamp = VALUES(timestamp), " +
                                                    "structureXP = VALUES(structureXP), " +
                                                    "creepXP = VALUES(creepXP), " +
                                                    "heroXP = VALUES(heroXP), " +
                                                    "minionXP = VALUES(minionXP), " +
                                                    "trickXP = VALUES(trickXP), " +
                                                    "totalXP = VALUES(totalXP)";

                                            //Console.WriteLine(cmd.CommandText);
                                            var writeReader = cmd.ExecuteReader();

                                        }
                                    }

                                }

                                //data = calculateMMR(data, conn);


                                for (var i = 0; i < data.Players.Length; i++)
                                {
                                    foreach (var item in data.Players[i].HeroLevelTaunt)
                                    {
                                        if (heroes_attr.ContainsKey(item.HeroAttributeId))
                                        {
                                            if (heroes_attr[item.HeroAttributeId] == data.Players[i].Hero)
                                            {
                                                data.Players[i].MasteyTauntTier = item.TierLevel;
                                                break;
                                            }
                                        }
                                    }


                                    for (var j = 0; j < data.Players.Length; j++)
                                    {
                                        if (j != i)
                                        {
                                            if (data.Players[i].Hero == data.Players[j].Hero)
                                            {
                                                data.Players[i].Mirror = 1;
                                                break;
                                            }
                                        }
                                    }
                                }


                                for (var i = 0; i < data.Players.Length; i++)
                                {
                                    if (data.Players[i].Winner)
                                    {
                                        data.Players[i].WinnerValue = "1";
                                    }
                                    else
                                    {
                                        data.Players[i].WinnerValue = "0";

                                    }
                                    //Console.WriteLine("Saving Player Information for:" + data.Players.Battletag);
                                    using (var cmd = conn.CreateCommand())
                                    {
                                        cmd.CommandText = "INSERT INTO player (" +
                                            "replayID, " +
                                            "blizz_id, " +
                                            "battletag, " +
                                            "hero, " +
                                            "hero_level, " +
                                            "mastery_taunt, " +
                                            "team, " +
                                            "winner, " +
                                            "party " +

                
                                            ") VALUES(" +
                                            replayID + "," +
                                            data.Players[i].BlizzId + "," +
                                            data.Players[i].battletag_table_id + "," +
                                            heroes[data.Players[i].Hero] + "," +
                                            data.Players[i].HeroLevel + "," +
                                            data.Players[i].MasteyTauntTier + "," +
                                            data.Players[i].Team + "," +
                                            data.Players[i].WinnerValue + "," +
                                            data.Players[i].Party +
                                            ")";
                                        cmd.CommandTimeout = 0;
                                        var Reader = cmd.ExecuteReader();
                                    }

                                    //Console.WriteLine("Saving Score Information for:" + data.Players.Battletag);
                                    if (data.Players[i].Score != null)
                                    {
                                        using (var cmd = conn.CreateCommand())
                                        {
                                            cmd.CommandText = "INSERT INTO scores (" +
                                                "replayID, " +
                                                "battletag, " +
                                                "level, " +
                                                "kills, " +
                                                "assists, " +
                                                "takedowns, " +
                                                "deaths, " +
                                                "highest_kill_streak, " +
                                                "hero_damage, " +
                                                "siege_damage, " +
                                                "structure_damage, " +
                                                "minion_damage, " +
                                                "creep_damage, " +
                                                "summon_damage, " +
                                                "time_cc_enemy_heroes, " +
                                                "healing, " +
                                                "self_healing, " +
                                                "damage_taken, " +
                                                "experience_contribution, " +
                                                "town_kills, " +
                                                "time_spent_dead, " +
                                                "merc_camp_captures, " +
                                                "watch_tower_captures, " +
                                                "meta_experience, " +
                                                "match_award, " +
                                                "protection_allies, " +
                                                "silencing_enemies, " +
                                                "rooting_enemies, " +
                                                "stunning_enemies, " +
                                                "clutch_heals, " +
                                                "escapes, " +
                                                "vengeance, " +
                                                "outnumbered_deaths, " +
                                                "teamfight_escapes, " +
                                                "teamfight_healing, " +
                                                "teamfight_damage_taken, " +
                                                "teamfight_hero_damage, " +
                                                "multikill, " +
                                                "physical_damage, " +
                                                "spell_damage," +
                                                "regen_globes," +
                                                "first_to_ten" +
                                                ") VALUES(" +
                                                replayID + "," +
                                                data.Players[i].battletag_table_id + "," +
                                                checkIfEmpty(data.Players[i].Score.Level) + "," +
                                                checkIfEmpty(data.Players[i].Score.SoloKills) + "," +
                                                checkIfEmpty(data.Players[i].Score.Assists) + "," +
                                                checkIfEmpty(data.Players[i].Score.Takedowns) + "," +
                                                checkIfEmpty(data.Players[i].Score.Deaths) + "," +
                                                checkIfEmpty(data.Players[i].Score.HighestKillStreak) + "," +
                                                checkIfEmpty(data.Players[i].Score.HeroDamage) + "," +
                                                checkIfEmpty(data.Players[i].Score.SiegeDamage) + "," +
                                                checkIfEmpty(data.Players[i].Score.StructureDamage) + "," +
                                                checkIfEmpty(data.Players[i].Score.MinionDamage) + "," +
                                                checkIfEmpty(data.Players[i].Score.CreepDamage) + "," +
                                                checkIfEmpty(data.Players[i].Score.SummonDamage) + "," +
                                                checkIfEmpty(Convert.ToInt64(data.Players[i].Score.TimeCCdEnemyHeroes_not_null.UtcDateTime.TimeOfDay.TotalSeconds)) + "," +
                                                checkIfEmpty(data.Players[i].Score.Healing) + "," +
                                                checkIfEmpty(data.Players[i].Score.SelfHealing) + "," +
                                                checkIfEmpty(data.Players[i].Score.DamageTaken) + "," +
                                                checkIfEmpty(data.Players[i].Score.ExperienceContribution) + "," +
                                                checkIfEmpty(data.Players[i].Score.TownKills) + "," +
                                                checkIfEmpty(Convert.ToInt64(data.Players[i].Score.TimeSpentDead.UtcDateTime.TimeOfDay.TotalSeconds)) + "," +

                                                checkIfEmpty(data.Players[i].Score.MercCampCaptures) + "," +
                                                checkIfEmpty(data.Players[i].Score.WatchTowerCaptures) + "," +
                                                checkIfEmpty(data.Players[i].Score.MetaExperience) + ",";
                                            if (data.Players[i].Score.MatchAwards.Length > 0)
                                            {
                                                cmd.CommandText += data.Players[i].Score.MatchAwards[0] + ",";

                                            }
                                            else
                                            {
                                                cmd.CommandText += "NULL" + ",";
                                            }

                                            cmd.CommandText += checkIfEmpty(data.Players[i].Score.ProtectionGivenToAllies) + "," +
                                            checkIfEmpty(data.Players[i].Score.TimeSilencingEnemyHeroes) + "," +
                                            checkIfEmpty(data.Players[i].Score.TimeRootingEnemyHeroes) + "," +
                                            checkIfEmpty(data.Players[i].Score.TimeStunningEnemyHeroes) + "," +
                                            checkIfEmpty(data.Players[i].Score.ClutchHealsPerformed) + "," +
                                            checkIfEmpty(data.Players[i].Score.EscapesPerformed) + "," +
                                            checkIfEmpty(data.Players[i].Score.VengeancesPerformed) + "," +
                                            checkIfEmpty(data.Players[i].Score.OutnumberedDeaths) + "," +
                                            checkIfEmpty(data.Players[i].Score.TeamfightEscapesPerformed) + "," +
                                            checkIfEmpty(data.Players[i].Score.TeamfightHealingDone) + "," +
                                            checkIfEmpty(data.Players[i].Score.TeamfightDamageTaken) + "," +
                                            checkIfEmpty(data.Players[i].Score.TeamfightHeroDamage) + "," +

                                            checkIfEmpty(data.Players[i].Score.Multikill) + "," +
                                            checkIfEmpty(data.Players[i].Score.PhysicalDamage) + "," +
                                            checkIfEmpty(data.Players[i].Score.SpellDamage) + "," +
                                            checkIfEmpty(data.Players[i].Score.RegenGlobes) + "," +
                                            checkIfEmpty(data.Players[i].Score.FirstToTen) + ")";




                                            cmd.CommandTimeout = 0;
                                            //Console.WriteLine(cmd.CommandText);
                                            var Reader = cmd.ExecuteReader();
                                        }
                                    }

                                    //Console.WriteLine("Saving Talent Information for:" + data.Players.Battletag);
                                    using (var cmd = conn.CreateCommand())
                                    {
                                        if (data.Players[i].Talents != null)
                                        {
                                            if (data.Players[i].Talents.Length == 0)
                                            {
                                                cmd.CommandText = "INSERT INTO talents (replayID, battletag, level_one, level_four, level_seven, level_ten, level_thirteen, level_sixteen, level_twenty) VALUES(" +
                                               replayID + "," +
                                               data.Players[i].battletag_table_id + "," +
                                               "NULL" + "," +
                                               "NULL" + "," +
                                               "NULL" + "," +
                                               "NULL" + "," +
                                               "NULL" + "," +
                                               "NULL" + "," +
                                               "NULL" + ")";
                                            }
                                            else if (data.Players[i].Talents.Length == 1)
                                            {
                                                cmd.CommandText = "INSERT INTO talents (replayID, battletag, level_one) VALUES(" +
                                                replayID + "," +
                                                data.Players[i].battletag_table_id + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[0]] + ")";
                                            }
                                            else if (data.Players[i].Talents.Length == 2)
                                            {
                                                cmd.CommandText = "INSERT INTO talents (replayID, battletag, level_one, level_four) VALUES(" +
                                                replayID + "," +
                                                data.Players[i].battletag_table_id + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[0]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[1]] + ")";
                                            }
                                            else if (data.Players[i].Talents.Length == 3)
                                            {
                                                cmd.CommandText = "INSERT INTO talents (replayID, battletag, level_one, level_four, level_seven) VALUES(" +
                                                replayID + "," +
                                                data.Players[i].battletag_table_id + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[0]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[1]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[2]] + ")";
                                            }
                                            else if (data.Players[i].Talents.Length == 4)
                                            {
                                                cmd.CommandText = "INSERT INTO talents (replayID, battletag, level_one, level_four, level_seven, level_ten) VALUES(" +
                                                replayID + "," +
                                                data.Players[i].battletag_table_id + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[0]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[1]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[2]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[3]] + ")";
                                            }
                                            else if (data.Players[i].Talents.Length == 5)
                                            {
                                                cmd.CommandText = "INSERT INTO talents (replayID, battletag, level_one, level_four, level_seven, level_ten, level_thirteen) VALUES(" +
                                                replayID + "," +
                                                data.Players[i].battletag_table_id + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[0]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[1]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[2]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[3]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[4]] + ")";
                                            }
                                            else if (data.Players[i].Talents.Length == 6)
                                            {
                                                cmd.CommandText = "INSERT INTO talents (replayID, battletag, level_one, level_four, level_seven, level_ten, level_thirteen, level_sixteen) VALUES(" +
                                                replayID + "," +
                                                data.Players[i].battletag_table_id + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[0]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[1]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[2]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[3]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[4]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[5]] + ")";
                                            }
                                            else if (data.Players[i].Talents.Length == 7)
                                            {
                                                cmd.CommandText = "INSERT INTO talents (replayID, battletag, level_one, level_four, level_seven, level_ten, level_thirteen, level_sixteen, level_twenty) VALUES(" +
                                                replayID + "," +
                                                data.Players[i].battletag_table_id + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[0]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[1]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[2]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[3]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[4]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[5]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[6]] + ")";
                                            }

                                            cmd.CommandTimeout = 0;
                                            //Console.WriteLine(cmd.CommandText);
                                            var Reader = cmd.ExecuteReader();
                                        }
                                        else
                                        {
                                            cmd.CommandText = "INSERT INTO talents (replayID, battletag, level_one, level_four, level_seven, level_ten, level_thirteen, level_sixteen, level_twenty) VALUES(" +
                                               replayID + "," +
                                               data.Players[i].battletag_table_id + "," +
                                               "NULL" + "," +
                                               "NULL" + "," +
                                               "NULL" + "," +
                                               "NULL" + "," +
                                               "NULL" + "," +
                                               "NULL" + "," +
                                               "NULL" + ")";

                                            cmd.CommandTimeout = 0;
                                            //Console.WriteLine(cmd.CommandText);
                                            var Reader = cmd.ExecuteReader();
                                        }

                                    }

                                }
                                //saveMasterMMRData(data, conn);

                                if (seasons_game_versions.ContainsKey(data.VersionSplit))
                                {
                                    if (Convert.ToInt32(seasons_game_versions[data.VersionSplit]) >= 13)
                                    {
                                        updateGameModeTotalGames(Convert.ToInt32(seasons_game_versions[data.VersionSplit]), data, conn);
                                        insertUrlIntoReplayUrls(data, conn);
                                    }
                                }
                                else
                                {
                                    var season = saveToSeasonGameVersion(DateTime.Parse(data.Date.ToString("yyyy-MM-dd HH:mm:ss")), data.VersionSplit, conn);
                                    seasons_game_versions.Add(data.VersionSplit, season);
                                    //Save Game Version to table
                                    //Add it to dic
                                    if (Convert.ToInt32(seasons_game_versions[data.VersionSplit]) >= 13)
                                    {
                                        updateGameModeTotalGames(Convert.ToInt32(seasons_game_versions[data.VersionSplit]), data, conn);
                                        insertUrlIntoReplayUrls(data, conn);
                                    }
                                }

                            }
                            else
                            {
                                if (badHeroName)
                                {
                                    Console.WriteLine("Bad Hero Name - Saving in Replays Not Processed");
                                    insertIntoReplaysNotProcessed(replayID.ToString(), "NULL", data.Region.ToString(), data.Mode, data.Length.UtcDateTime.TimeOfDay.TotalSeconds.ToString(), data.Date.ToString("yyyy-MM-dd HH:mm:ss"), data.Map.ToString(), data.VersionSplit.ToString(), "0", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), replayURL.ToString(), "NULL", "Bad Hero Name");

                                    //save as bad hero name
                                }
                                else if (badTalentName)
                                {
                                    Console.WriteLine("Bad Talent Name - Saving in Replays Not Processed");
                                    insertIntoReplaysNotProcessed(replayID.ToString(), "NULL", data.Region.ToString(), data.Mode, data.Length.UtcDateTime.TimeOfDay.TotalSeconds.ToString(), data.Date.ToString("yyyy-MM-dd HH:mm:ss"), data.Map.ToString(), data.VersionSplit.ToString(), "0", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), replayURL.ToString(), "NULL", "Bad Talent Name");

                                    //save as bad talent name
                                }
                                else
                                {
                                    Console.WriteLine("Unknown Failure - Saving in Replays Not Processed");
                                    //Save in replays not processed
                                    insertIntoReplaysNotProcessed(replayID.ToString(), "NULL", data.Region.ToString(), data.Mode, data.Length.UtcDateTime.TimeOfDay.TotalSeconds.ToString(), data.Date.ToString("yyyy-MM-dd HH:mm:ss"), data.Map.ToString(), data.VersionSplit.ToString(), "0", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), replayURL.ToString(), "NULL", "Undetermined");


                                }

                            }


                        }
                    }

                }

            }
            catch (Exception e)
            {
                using (var conn = new MySqlConnection(db_connect_string))
                {
                    conn.Open();
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                                            replayID + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + 0 + "\"" + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + "0" + "\"" + "," +
                                            "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                            1 + "," +
                                            "\"" + replayURL + "\"" + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + e.ToString() + "\"" + ")";
                        cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                                            "replayId = VALUES(replayId), " +
                                            "parsedID = VALUES(parsedID)," +
                                            "region = VALUES(region)," +
                                            "game_type = VALUES(game_type)," +
                                            "game_length = VALUES(game_length)," +
                                            "game_date = VALUES(game_date)," +
                                            "game_map = VALUES(game_map)," +
                                            "game_version = VALUES(game_version)," +
                                            "size = VALUES(size)," +
                                            "date_parsed = VALUES(date_parsed)," +
                                            "count_parsed = count_parsed + VALUES(count_parsed), " +
                                            "url = VALUES(url)," +
                                            "processed = VALUES(processed)," +
                                            "failure_status = VALUES(failure_status)";
                        cmd.CommandTimeout = 0;
                        //Console.WriteLine(cmd.CommandText);
                        var Reader = cmd.ExecuteReader();
                    }
                }
            }

        }

        public void saveReplayDataBrawl(LambdaJson.ReplayData data)
        {
            try
            {
                data.GameType_id = "-1";
                if (data.Players != null)
                {
                    using (var conn = new MySqlConnection(db_connect_string))
                    {
                        conn.Open();
                        var badHeroName = false;
                        var badTalentName = false;
                        if (data.Players.Length == 10)
                        {
                            for (var i = 0; i < data.Players.Length; i++)
                            {
                                if (data.Players[i].Hero == null)
                                {
                                    if (data.Players[i].Talents != null)
                                    {
                                        var split = Regex.Split(data.Players[i].Talents[0], @"(?<!^)(?=[A-Z])");

                                        if (heroes.ContainsKey(split[0]))
                                        {
                                            data.Players[i].Hero = split[0];
                                            data.Players[i].Hero_id = heroes[split[0]];
                                        }
                                        else
                                        {
                                            badHeroName = true;
                                            break;

                                        }
                                    }
                                    else
                                    {
                                        badHeroName = true;
                                        break;
                                    }


                                }
                                else
                                {
                                    if (!heroes.ContainsKey(data.Players[i].Hero))
                                    {
                                        Console.WriteLine(data.Players[i].Hero);
                                        Console.WriteLine(data.Players[i].Talents[0]);
                                        badHeroName = true;
                                        break;
                                    }
                                    else
                                    {
                                        data.Players[i].Hero_id = heroes[data.Players[i].Hero];
                                    }
                                }


                                if (data.Players[i].Talents != null)
                                {
                                    for (var t = 0; t < data.Players[i].Talents.Length; t++)
                                    {
                                        if (!talents.ContainsKey(data.Players[i].Hero + "|" + data.Players[i].Talents[t]))
                                        {
                                            talents.Add(data.Players[i].Hero + "|" + data.Players[i].Talents[t], insertIntoTalentTable(data.Players[i].Hero, data.Players[i].Talents[t], conn));
                                        }
                                    }

                                }
                            }
                            var team_one_level_ten_time = DateTimeOffset.Now;
                            var team_two_level_ten_time = DateTimeOffset.Now;

                            for (var teams = 0; teams < data.TeamExperience.Length; teams++)
                            {
                                for (var team_time_split = 0; team_time_split < data.TeamExperience[teams].Length; team_time_split++)
                                {
                                    if (data.TeamExperience[teams][team_time_split].TeamLevel >= 10)
                                    {
                                        if (teams == 0)
                                        {
                                            team_one_level_ten_time = data.TeamExperience[teams][team_time_split].TimeSpan;
                                            break;

                                        }
                                        else
                                        {
                                            team_two_level_ten_time = data.TeamExperience[teams][team_time_split].TimeSpan;
                                            break;

                                        }
                                    }
                                }
                            }
                            var team_one_first_to_ten = 0;
                            var team_two_first_to_ten = 0;
                            if (team_one_level_ten_time < team_two_level_ten_time)
                            {
                                team_one_first_to_ten = 1;
                            }
                            else
                            {
                                team_two_first_to_ten = 1;
                            }

                            for (var i = 0; i < data.Players.Length; i++)
                            {
                                if (data.Players[i].Team == 0)
                                {
                                    data.Players[i].Score.FirstToTen = team_one_first_to_ten;
                                }
                                else
                                {
                                    data.Players[i].Score.FirstToTen = team_two_first_to_ten;

                                }
                            }


                            if (!badHeroName)
                            {

                                for (var i = 0; i < data.Players.Length; i++)
                                {

                                    //Console.WriteLine("Adding Battletag to battletag table for: " + data.Players.Battletag);

                                    using (var cmd = conn.CreateCommand())
                                    {
                                        cmd.CommandText = "INSERT INTO battletags (blizz_id, battletag, region, account_level, latest_game) VALUES " +
                                            "(" + data.Players[i].BlizzId + "," +
                                            "\"" + data.Players[i].BattletagName + "#" + data.Players[i].BattletagId + "\"" + "," +
                                            data.Region + "," +
                                            data.Players[i].AccountLevel + "," +
                                            "\"" + data.Date.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + ")";

                                        cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                                                            "blizz_id = VALUES(blizz_id), " +
                                                            "battletag = VALUES(battletag), " +
                                                            "region = VALUES(region), " +
                                                            "account_level = IF(VALUES(account_level) > account_level, VALUES(account_level), account_level), " +
                                                            "latest_game = IF(VALUES(latest_game) > latest_game, VALUES(latest_game), latest_game) ";
                                        cmd.CommandTimeout = 0;



                                        var Reader = cmd.ExecuteReader();
                                    }
                                    //Console.WriteLine("Getting player_id for: " + data.Players.Battletag);


                                    using (var cmd = conn.CreateCommand())
                                    {

                                        cmd.CommandText = "SELECT player_id from battletags where battletag = " + "\"" + data.Players[i].BattletagName + "#" + data.Players[i].BattletagId + "\"";
                                        cmd.CommandTimeout = 0;
                                        var Reader = cmd.ExecuteReader();

                                        while (Reader.Read())
                                        {
                                            data.Players[i].battletag_table_id = Reader.GetString("player_id");
                                        }
                                    }
                                }



                                //Console.WriteLine("Saving Replay Information for:" + data.Id);

                                using (var cmd = conn.CreateCommand())
                                {
                                    cmd.CommandText = "INSERT INTO heroesprofile_brawl.replay (replayID, game_date, game_length, game_map, game_version, region, date_added) VALUES(" +
                                        replayID + "," +
                                        "\"" + data.Date.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                        data.Length.UtcDateTime.TimeOfDay.TotalSeconds + "," +
                                        maps[data.Map] + "," +
                                        "\"" + data.VersionSplit + "\"" + "," +
                                        data.Region + "," +
                                        "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + ")";
                                    cmd.CommandTimeout = 0;
                                    //Console.WriteLine(cmd.CommandText);
                                    var Reader = cmd.ExecuteReader();
                                }


                                using (var cmd = conn.CreateCommand())
                                {
                                    cmd.CommandText = "DELETE FROM replays_not_processed WHERE replayID = " + replayID;
                                    var Reader = cmd.ExecuteReader();
                                }

                                for (var i = 0; i < data.Players.Length; i++)
                                {
                                    foreach (var item in data.Players[i].HeroLevelTaunt)
                                    {
                                        if (heroes_attr.ContainsKey(item.HeroAttributeId))
                                        {
                                            if (heroes_attr[item.HeroAttributeId] == data.Players[i].Hero)
                                            {
                                                data.Players[i].MasteyTauntTier = item.TierLevel;
                                                break;
                                            }
                                        }
                                    }
                                    for (var j = 0; j < data.Players.Length; j++)
                                    {
                                        if (j != i)
                                        {
                                            if (data.Players[i].Hero == data.Players[j].Hero)
                                            {
                                                data.Players[i].Mirror = 1;
                                            }
                                        }
                                    }
                                }

                                for (var i = 0; i < data.Players.Length; i++)
                                {
                                    if (data.Players[i].Winner)
                                    {
                                        data.Players[i].WinnerValue = "1";
                                    }
                                    else
                                    {
                                        data.Players[i].WinnerValue = "0";

                                    }


                                    using (var cmd = conn.CreateCommand())
                                    {
                                        cmd.CommandText = "INSERT INTO heroesprofile_brawl.player (" +
                                            "replayID, " +
                                            "blizz_id, " +
                                            "battletag, " +
                                            "hero, " +
                                            "hero_level, " +
                                            "mastery_taunt, " +
                                            "team, " +
                                            "winner, " +
                                            "party " +
                                            ") VALUES(" +
                                            replayID + "," +
                                            data.Players[i].BlizzId + "," +
                                            data.Players[i].battletag_table_id + "," +
                                            heroes[data.Players[i].Hero] + "," +
                                            data.Players[i].HeroLevel + "," +
                                            data.Players[i].MasteyTauntTier + "," +
                                            data.Players[i].Team + "," +
                                            data.Players[i].WinnerValue + "," +
                                            data.Players[i].Party + ")";
                                        cmd.CommandTimeout = 0;
                                        var Reader = cmd.ExecuteReader();
                                    }

                                    if (data.Players[i].Score != null)
                                    {
                                        using (var cmd = conn.CreateCommand())
                                        {
                                            cmd.CommandText = "INSERT INTO heroesprofile_brawl.scores (" +
                                                "replayID, " +
                                                "battletag, " +
                                                "level, " +
                                                "kills, " +
                                                "assists, " +
                                                "takedowns, " +
                                                "deaths, " +
                                                "highest_kill_streak, " +
                                                "hero_damage, " +
                                                "siege_damage, " +
                                                "structure_damage, " +
                                                "minion_damage, " +
                                                "creep_damage, " +
                                                "summon_damage, " +
                                                "time_cc_enemy_heroes, " +
                                                "healing, " +
                                                "self_healing, " +
                                                "damage_taken, " +
                                                "experience_contribution, " +
                                                "town_kills, " +
                                                "time_spent_dead, " +
                                                "merc_camp_captures, " +
                                                "watch_tower_captures, " +
                                                "meta_experience, " +
                                                "match_award, " +
                                                "protection_allies, " +
                                                "silencing_enemies, " +
                                                "rooting_enemies, " +
                                                "stunning_enemies, " +
                                                "clutch_heals, " +
                                                "escapes, " +
                                                "vengeance, " +
                                                "outnumbered_deaths, " +
                                                "teamfight_escapes, " +
                                                "teamfight_healing, " +
                                                "teamfight_damage_taken, " +
                                                "teamfight_hero_damage, " +
                                                "multikill, " +
                                                "physical_damage, " +
                                                "spell_damage," +
                                                "regen_globes," +
                                                "first_to_ten" +
                                                ") VALUES(" +
                                                replayID + "," +
                                                data.Players[i].battletag_table_id + "," +
                                                checkIfEmpty(data.Players[i].Score.Level) + "," +
                                                checkIfEmpty(data.Players[i].Score.SoloKills) + "," +
                                                checkIfEmpty(data.Players[i].Score.Assists) + "," +
                                                checkIfEmpty(data.Players[i].Score.Takedowns) + "," +
                                                checkIfEmpty(data.Players[i].Score.Deaths) + "," +
                                                checkIfEmpty(data.Players[i].Score.HighestKillStreak) + "," +
                                                checkIfEmpty(data.Players[i].Score.HeroDamage) + "," +
                                                checkIfEmpty(data.Players[i].Score.SiegeDamage) + "," +
                                                checkIfEmpty(data.Players[i].Score.StructureDamage) + "," +
                                                checkIfEmpty(data.Players[i].Score.MinionDamage) + "," +
                                                checkIfEmpty(data.Players[i].Score.CreepDamage) + "," +
                                                checkIfEmpty(data.Players[i].Score.SummonDamage) + "," +
                                                checkIfEmpty(Convert.ToInt64(data.Players[i].Score.TimeCCdEnemyHeroes_not_null.UtcDateTime.TimeOfDay.TotalSeconds)) + "," +
                                                checkIfEmpty(data.Players[i].Score.Healing) + "," +
                                                checkIfEmpty(data.Players[i].Score.SelfHealing) + "," +
                                                checkIfEmpty(data.Players[i].Score.DamageTaken) + "," +
                                                checkIfEmpty(data.Players[i].Score.ExperienceContribution) + "," +
                                                checkIfEmpty(data.Players[i].Score.TownKills) + "," +
                                                checkIfEmpty(Convert.ToInt64(data.Players[i].Score.TimeSpentDead.UtcDateTime.TimeOfDay.TotalSeconds)) + "," +

                                                checkIfEmpty(data.Players[i].Score.MercCampCaptures) + "," +
                                                checkIfEmpty(data.Players[i].Score.WatchTowerCaptures) + "," +
                                                checkIfEmpty(data.Players[i].Score.MetaExperience) + ",";
                                            if (data.Players[i].Score.MatchAwards.Length > 0)
                                            {
                                                cmd.CommandText += data.Players[i].Score.MatchAwards[0] + ",";

                                            }
                                            else
                                            {
                                                cmd.CommandText += "NULL" + ",";
                                            }

                                            cmd.CommandText += checkIfEmpty(data.Players[i].Score.ProtectionGivenToAllies) + "," +
                                            checkIfEmpty(data.Players[i].Score.TimeSilencingEnemyHeroes) + "," +
                                            checkIfEmpty(data.Players[i].Score.TimeRootingEnemyHeroes) + "," +
                                            checkIfEmpty(data.Players[i].Score.TimeStunningEnemyHeroes) + "," +
                                            checkIfEmpty(data.Players[i].Score.ClutchHealsPerformed) + "," +
                                            checkIfEmpty(data.Players[i].Score.EscapesPerformed) + "," +
                                            checkIfEmpty(data.Players[i].Score.VengeancesPerformed) + "," +
                                            checkIfEmpty(data.Players[i].Score.OutnumberedDeaths) + "," +
                                            checkIfEmpty(data.Players[i].Score.TeamfightEscapesPerformed) + "," +
                                            checkIfEmpty(data.Players[i].Score.TeamfightHealingDone) + "," +
                                            checkIfEmpty(data.Players[i].Score.TeamfightDamageTaken) + "," +
                                            checkIfEmpty(data.Players[i].Score.TeamfightHeroDamage) + "," +

                                            checkIfEmpty(data.Players[i].Score.Multikill) + "," +
                                            checkIfEmpty(data.Players[i].Score.PhysicalDamage) + "," +
                                            checkIfEmpty(data.Players[i].Score.SpellDamage) + "," +
                                            checkIfEmpty(data.Players[i].Score.RegenGlobes) + "," +
                                            checkIfEmpty(data.Players[i].Score.FirstToTen) + ")";




                                            cmd.CommandTimeout = 0;
                                            //Console.WriteLine(cmd.CommandText);
                                            var Reader = cmd.ExecuteReader();
                                        }
                                    }

                                    //Console.WriteLine("Saving Talent Information for:" + data.Players.Battletag);
                                    using (var cmd = conn.CreateCommand())
                                    {
                                        if (data.Players[i].Talents != null)
                                        {
                                            if (data.Players[i].Talents.Length == 0)
                                            {
                                                cmd.CommandText = "INSERT INTO heroesprofile_brawl.talents (replayID, battletag, level_one, level_four, level_seven, level_ten, level_thirteen, level_sixteen, level_twenty) VALUES(" +
                                               replayID + "," +
                                               data.Players[i].battletag_table_id + "," +
                                               "NULL" + "," +
                                               "NULL" + "," +
                                               "NULL" + "," +
                                               "NULL" + "," +
                                               "NULL" + "," +
                                               "NULL" + "," +
                                               "NULL" + ")";
                                            }
                                            else if (data.Players[i].Talents.Length == 1)
                                            {
                                                cmd.CommandText = "INSERT INTO heroesprofile_brawl.talents (replayID, battletag, level_one) VALUES(" +
                                                replayID + "," +
                                                data.Players[i].battletag_table_id + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[0]] + ")";
                                            }
                                            else if (data.Players[i].Talents.Length == 2)
                                            {
                                                cmd.CommandText = "INSERT INTO heroesprofile_brawl.talents (replayID, battletag, level_one, level_four) VALUES(" +
                                                replayID + "," +
                                                data.Players[i].battletag_table_id + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[0]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[1]] + ")";
                                            }
                                            else if (data.Players[i].Talents.Length == 3)
                                            {
                                                cmd.CommandText = "INSERT INTO heroesprofile_brawl.talents (replayID, battletag, level_one, level_four, level_seven) VALUES(" +
                                                replayID + "," +
                                                data.Players[i].battletag_table_id + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[0]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[1]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[2]] + ")";
                                            }
                                            else if (data.Players[i].Talents.Length == 4)
                                            {
                                                cmd.CommandText = "INSERT INTO heroesprofile_brawl.talents (replayID, battletag, level_one, level_four, level_seven, level_ten) VALUES(" +
                                                replayID + "," +
                                                data.Players[i].battletag_table_id + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[0]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[1]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[2]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[3]] + ")";
                                            }
                                            else if (data.Players[i].Talents.Length == 5)
                                            {
                                                cmd.CommandText = "INSERT INTO heroesprofile_brawl.talents (replayID, battletag, level_one, level_four, level_seven, level_ten, level_thirteen) VALUES(" +
                                                replayID + "," +
                                                data.Players[i].battletag_table_id + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[0]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[1]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[2]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[3]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[4]] + ")";
                                            }
                                            else if (data.Players[i].Talents.Length == 6)
                                            {
                                                cmd.CommandText = "INSERT INTO heroesprofile_brawl.talents (replayID, battletag, level_one, level_four, level_seven, level_ten, level_thirteen, level_sixteen) VALUES(" +
                                                replayID + "," +
                                                data.Players[i].battletag_table_id + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[0]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[1]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[2]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[3]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[4]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[5]] + ")";
                                            }
                                            else if (data.Players[i].Talents.Length == 7)
                                            {
                                                cmd.CommandText = "INSERT INTO heroesprofile_brawl.talents (replayID, battletag, level_one, level_four, level_seven, level_ten, level_thirteen, level_sixteen, level_twenty) VALUES(" +
                                                replayID + "," +
                                                data.Players[i].battletag_table_id + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[0]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[1]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[2]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[3]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[4]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[5]] + "," +
                                                talents[data.Players[i].Hero + "|" + data.Players[i].Talents[6]] + ")";
                                            }

                                            cmd.CommandTimeout = 0;
                                            //Console.WriteLine(cmd.CommandText);
                                            var Reader = cmd.ExecuteReader();
                                        }
                                        else
                                        {
                                            cmd.CommandText = "INSERT INTO heroesprofile_brawl.talents (replayID, battletag, level_one, level_four, level_seven, level_ten, level_thirteen, level_sixteen, level_twenty) VALUES(" +
                                               replayID + "," +
                                               data.Players[i].battletag_table_id + "," +
                                               "NULL" + "," +
                                               "NULL" + "," +
                                               "NULL" + "," +
                                               "NULL" + "," +
                                               "NULL" + "," +
                                               "NULL" + "," +
                                               "NULL" + ")";

                                            cmd.CommandTimeout = 0;
                                            //Console.WriteLine(cmd.CommandText);
                                            var Reader = cmd.ExecuteReader();
                                        }

                                    }

                                }

                                if (seasons_game_versions.ContainsKey(data.VersionSplit))
                                {
                                    if (Convert.ToInt32(seasons_game_versions[data.VersionSplit]) >= 13)
                                    {
                                        updateGlobalHeroData(data, conn);
                                        updateGlobalTalentData(data, conn);
                                        updateGlobalTalentDataDetails(data, conn);
                                    }
                                }
                                else
                                {
                                    var season = saveToSeasonGameVersion(DateTime.Parse(data.Date.ToString("yyyy-MM-dd HH:mm:ss")), data.VersionSplit, conn);
                                    seasons_game_versions.Add(data.VersionSplit, season);
                                    //Save Game Version to table
                                    //Add it to dic
                                    if (Convert.ToInt32(seasons_game_versions[data.VersionSplit]) >= 13)
                                    {
                                        updateGlobalHeroData(data, conn);
                                        updateGlobalTalentData(data, conn);
                                        updateGlobalTalentDataDetails(data, conn);
                                    }
                                }

                            }
                            else
                            {
                                if (badHeroName)
                                {
                                    Console.WriteLine("Bad Hero Name - Saving in Replays Not Processed");
                                    insertIntoReplaysNotProcessed(replayID.ToString(), "NULL", data.Region.ToString(), data.Mode, data.Length.UtcDateTime.TimeOfDay.TotalSeconds.ToString(), data.Date.ToString("yyyy-MM-dd HH:mm:ss"), data.Map.ToString(), data.VersionSplit.ToString(), "0", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), replayURL.ToString(), "NULL", "Bad Hero Name");

                                    //save as bad hero name
                                }
                                else if (badTalentName)
                                {
                                    Console.WriteLine("Bad Talent Name - Saving in Replays Not Processed");
                                    insertIntoReplaysNotProcessed(replayID.ToString(), "NULL", data.Region.ToString(), data.Mode, data.Length.UtcDateTime.TimeOfDay.TotalSeconds.ToString(), data.Date.ToString("yyyy-MM-dd HH:mm:ss"), data.Map.ToString(), data.VersionSplit.ToString(), "0", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), replayURL.ToString(), "NULL", "Bad Talent Name");

                                    //save as bad talent name
                                }
                                else
                                {
                                    Console.WriteLine("Unknown Failure - Saving in Replays Not Processed");
                                    //Save in replays not processed
                                    insertIntoReplaysNotProcessed(replayID.ToString(), "NULL", data.Region.ToString(), data.Mode, data.Length.UtcDateTime.TimeOfDay.TotalSeconds.ToString(), data.Date.ToString("yyyy-MM-dd HH:mm:ss"), data.Map.ToString(), data.VersionSplit.ToString(), "0", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), replayURL.ToString(), "NULL", "Undetermined");


                                }

                            }


                        }
                    }

                }

            }
            catch (Exception e)
            {
                using (var conn = new MySqlConnection(db_connect_string))
                {
                    conn.Open();
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                                            replayID + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + 0 + "\"" + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + "0" + "\"" + "," +
                                            "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                            1 + "," +
                                            "\"" + replayURL + "\"" + "," +
                                            "\"" + "NULL" + "\"" + "," +
                                            "\"" + e.ToString() + "\"" + ")";
                        cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                                            "replayId = VALUES(replayId), " +
                                            "parsedID = VALUES(parsedID)," +
                                            "region = VALUES(region)," +
                                            "game_type = VALUES(game_type)," +
                                            "game_length = VALUES(game_length)," +
                                            "game_date = VALUES(game_date)," +
                                            "game_map = VALUES(game_map)," +
                                            "game_version = VALUES(game_version)," +
                                            "size = VALUES(size)," +
                                            "date_parsed = VALUES(date_parsed)," +
                                            "count_parsed = count_parsed + VALUES(count_parsed), " +
                                            "url = VALUES(url)," +
                                            "processed = VALUES(processed)," +
                                            "failure_status = VALUES(failure_status)";
                        cmd.CommandTimeout = 0;
                        //Console.WriteLine(cmd.CommandText);
                        var Reader = cmd.ExecuteReader();
                    }
                }
            }

        }


        private string checkIfEmpty(long? value)
        {
            if (value == null)
            {
                return "NULL";
            }
            else
            {
                return value.ToString();
            }
        }

        private void updateGlobalHeroData(LambdaJson.ReplayData data, MySqlConnection conn)
        {
            //

            for (var i = 0; i < data.Players.Length; i++)
            {

                var win_loss = 0;
                if (data.Players[i].Winner)
                {

                    win_loss = 1;
                }
                else
                {

                    win_loss = 0;
                }




                if (data.Players[i].Score != null)
                {
                    var hero_level = 0;

                    if (data.Players[i].HeroLevel < 5)
                    {
                        hero_level = 1;
                        hero_level = 1;

                    }
                    else if (data.Players[i].HeroLevel >= 5 && data.Players[i].HeroLevel < 10)
                    {
                        hero_level = 5;
                    }
                    else if (data.Players[i].HeroLevel >= 10 && data.Players[i].HeroLevel < 15)
                    {
                        hero_level = 10;
                    }
                    else if (data.Players[i].HeroLevel >= 15 && data.Players[i].HeroLevel < 20)
                    {
                        hero_level = 15;
                    }
                    else if (data.Players[i].HeroLevel >= 20)
                    {
                        hero_level = 20;
                    }

                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "INSERT INTO global_hero_stats (" +
                       "game_version, " +
                       "game_type, " +
                       "league_tier, " +
                       "hero_league_tier, " +
                       "role_league_tier," +
                       "game_map, " +
                            "hero_level, " +
                            "hero, " +
                            "mirror, " +
                            "win_loss, " +
                            "game_time, " +
                            "kills, " +
                            "assists, " +
                            "takedowns, " +
                            "deaths, " +
                            "highest_kill_streak, " +
                            "hero_damage, " +
                            "siege_damage, " +
                            "structure_damage, " +
                            "minion_damage, " +
                            "creep_damage, " +
                            "summon_damage, " +
                            "time_cc_enemy_heroes, " +
                            "healing, " +
                            "self_healing, " +
                            "damage_taken, " +
                            "experience_contribution, " +
                            "town_kills, " +
                            "time_spent_dead, " +
                            "merc_camp_captures, " +
                            "watch_tower_captures, " +
                            "protection_allies, " +
                            "silencing_enemies, " +
                            "rooting_enemies, " +
                            "stunning_enemies, " +
                            "clutch_heals, " +
                            "escapes, " +
                            "vengeance, " +
                            "outnumbered_deaths, " +
                            "teamfight_escapes, " +
                            "teamfight_healing, " +
                            "teamfight_damage_taken, " +
                            "teamfight_hero_damage, " +
                            "multikill, " +
                            "physical_damage, " +
                            "spell_damage, " +
                            "regen_globes, " +
                            "games_played" +
                            ") VALUES (" +
                            "\"" + data.VersionSplit + "\"" + ",";
                        if (data.Mode != "Brawl")
                        {
                            cmd.CommandText += "\"" + data.GameType_id + "\"" + "," +
                            "\"" + data.Players[i].player_league_tier + "\"" + ",";
                        }
                        else
                        {
                            cmd.CommandText += "\"" + "-1" + "\"" + "," +
                            "\"" + "0" + "\"" + "," +
                            "\"" + "0" + "\"" + "," +
                            "\"" + "0" + "\"" + ",";
                        }
                        cmd.CommandText += "\"" + data.GameMap_id + "\"" + "," +
                            "\"" + hero_level + "\"" + "," +
                            "\"" + data.Players[i].Hero_id + "\"" + "," +
                            "\"" + data.Players[i].Mirror + "\"" + "," +
                            "\"" + win_loss + "\"" + "," +

                            "\"" + data.Length.UtcDateTime.TimeOfDay.TotalSeconds + "\"" + "," +
                                        checkIfEmpty(data.Players[i].Score.SoloKills) + "," +
                                        checkIfEmpty(data.Players[i].Score.Assists) + "," +
                                        checkIfEmpty(data.Players[i].Score.Takedowns) + "," +
                                        checkIfEmpty(data.Players[i].Score.Deaths) + "," +
                                        checkIfEmpty(data.Players[i].Score.HighestKillStreak) + "," +
                                        checkIfEmpty(data.Players[i].Score.HeroDamage) + "," +
                                        checkIfEmpty(data.Players[i].Score.SiegeDamage) + "," +
                                        checkIfEmpty(data.Players[i].Score.StructureDamage) + "," +
                                        checkIfEmpty(data.Players[i].Score.MinionDamage) + "," +
                                        checkIfEmpty(data.Players[i].Score.CreepDamage) + "," +
                                        checkIfEmpty(data.Players[i].Score.SummonDamage) + "," +
                                        checkIfEmpty(Convert.ToInt64(data.Players[i].Score.TimeCCdEnemyHeroes_not_null.UtcDateTime.TimeOfDay.TotalSeconds)) + "," +
                                        checkIfEmpty(data.Players[i].Score.Healing) + "," +
                                        checkIfEmpty(data.Players[i].Score.SelfHealing) + "," +
                                        checkIfEmpty(data.Players[i].Score.DamageTaken) + "," +
                                        checkIfEmpty(data.Players[i].Score.ExperienceContribution) + "," +
                                        checkIfEmpty(data.Players[i].Score.TownKills) + "," +
                                        checkIfEmpty(Convert.ToInt64(data.Players[i].Score.TimeSpentDead.UtcDateTime.TimeOfDay.TotalSeconds)) + "," +
                                        checkIfEmpty(data.Players[i].Score.MercCampCaptures) + "," +
                                        checkIfEmpty(data.Players[i].Score.WatchTowerCaptures) + "," +
                                        checkIfEmpty(data.Players[i].Score.ProtectionGivenToAllies) + "," +
                                        checkIfEmpty(data.Players[i].Score.TimeSilencingEnemyHeroes) + "," +
                                        checkIfEmpty(data.Players[i].Score.TimeRootingEnemyHeroes) + "," +
                                        checkIfEmpty(data.Players[i].Score.TimeStunningEnemyHeroes) + "," +
                                        checkIfEmpty(data.Players[i].Score.ClutchHealsPerformed) + "," +
                                        checkIfEmpty(data.Players[i].Score.EscapesPerformed) + "," +
                                        checkIfEmpty(data.Players[i].Score.VengeancesPerformed) + "," +
                                        checkIfEmpty(data.Players[i].Score.OutnumberedDeaths) + "," +
                                        checkIfEmpty(data.Players[i].Score.TeamfightEscapesPerformed) + "," +
                                        checkIfEmpty(data.Players[i].Score.TeamfightHealingDone) + "," +
                                        checkIfEmpty(data.Players[i].Score.TeamfightDamageTaken) + "," +
                                        checkIfEmpty(data.Players[i].Score.TeamfightHeroDamage) + "," +

                                        checkIfEmpty(data.Players[i].Score.Multikill) + "," +
                                        checkIfEmpty(data.Players[i].Score.PhysicalDamage) + "," +
                                        checkIfEmpty(data.Players[i].Score.SpellDamage) + "," +
                                        checkIfEmpty(data.Players[i].Score.RegenGlobes) + "," +
                                        1 + ")";


                        cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                            "game_time = game_time + VALUES(game_time), " +
                            "kills = kills + VALUES(kills), " +
                            "assists = assists + VALUES(assists), " +
                            "takedowns = takedowns + VALUES(takedowns), " +
                            "deaths = deaths + VALUES(deaths), " +
                            "highest_kill_streak = highest_kill_streak + VALUES(highest_kill_streak), " +
                            "hero_damage = hero_damage + VALUES(hero_damage), " +
                            "siege_damage = siege_damage + VALUES(siege_damage), " +
                            "structure_damage = structure_damage + VALUES(structure_damage), " +
                            "minion_damage = minion_damage + VALUES(minion_damage), " +
                            "creep_damage = creep_damage + VALUES(creep_damage), " +
                            "summon_damage = summon_damage + VALUES(summon_damage), " +
                            "time_cc_enemy_heroes = time_cc_enemy_heroes + VALUES(time_cc_enemy_heroes), " +
                            "healing = healing + VALUES(healing), " +
                            "self_healing = self_healing + VALUES(self_healing), " +
                            "damage_taken = damage_taken + VALUES(damage_taken), " +
                            "experience_contribution = experience_contribution + VALUES(experience_contribution), " +
                            "town_kills = town_kills + VALUES(town_kills), " +
                            "time_spent_dead = time_spent_dead + VALUES(time_spent_dead), " +
                            "merc_camp_captures = merc_camp_captures + VALUES(merc_camp_captures), " +
                            "watch_tower_captures = watch_tower_captures + VALUES(watch_tower_captures), " +
                            "protection_allies = protection_allies + VALUES(protection_allies), " +
                            "silencing_enemies = silencing_enemies + VALUES(silencing_enemies), " +
                            "rooting_enemies = rooting_enemies + VALUES(rooting_enemies), " +
                            "stunning_enemies = stunning_enemies + VALUES(stunning_enemies), " +
                            "clutch_heals = clutch_heals + VALUES(clutch_heals), " +
                            "escapes = escapes + VALUES(escapes), " +
                            "vengeance = vengeance + VALUES(vengeance), " +
                            "outnumbered_deaths = outnumbered_deaths + VALUES(outnumbered_deaths), " +
                            "teamfight_escapes = teamfight_escapes + VALUES(teamfight_escapes), " +
                            "teamfight_healing = teamfight_healing + VALUES(teamfight_healing), " +
                            "teamfight_damage_taken = teamfight_damage_taken + VALUES(teamfight_damage_taken), " +
                            "teamfight_hero_damage = teamfight_hero_damage + VALUES(teamfight_hero_damage), " +
                            "multikill = multikill + VALUES(multikill), " +
                            "physical_damage = physical_damage + VALUES(physical_damage), " +
                            "spell_damage = spell_damage + VALUES(spell_damage), " +
                            "regen_globes = regen_globes + VALUES(regen_globes), " +
                            "games_played = games_played + VALUES(games_played)";
                        //Console.WriteLine(cmd.CommandText);
                        var Reader = cmd.ExecuteReader();
                    }
                }
            }
        }

        private void updateMatchups(LambdaJson.ReplayData data, MySqlConnection conn)
        {
            for (var i = 0; i < data.Players.Length; i++)
            {

                var win_loss = 0;
                if (data.Players[i].Winner)
                {

                    win_loss = 1;
                }
                else
                {

                    win_loss = 0;
                }




                if (data.Players[i].Score != null)
                {
                    var hero_level = 0;

                    if (data.Players[i].HeroLevel < 5)
                    {
                        hero_level = 1;

                    }
                    else if (data.Players[i].HeroLevel >= 5 && data.Players[i].HeroLevel < 10)
                    {
                        hero_level = 5;
                    }
                    else if (data.Players[i].HeroLevel >= 10 && data.Players[i].HeroLevel < 15)
                    {
                        hero_level = 10;
                    }
                    else if (data.Players[i].HeroLevel >= 15 && data.Players[i].HeroLevel < 20)
                    {
                        hero_level = 15;
                    }
                    else if (data.Players[i].HeroLevel >= 20)
                    {
                        hero_level = 20;
                    }

                    for (var j = 0; j < data.Players.Length; j++)
                    {
                        if (data.Players[j].BlizzId != data.Players[i].BlizzId)
                        {
                            using (var cmd = conn.CreateCommand())
                            {
                                if (data.Players[j].Team == data.Players[i].Team)
                                {
                                    cmd.CommandText = "INSERT INTO global_hero_matchups_ally (game_version, game_type, league_tier , game_map, hero_level, hero, ally, mirror, win_loss, games_played) VALUES (";

                                }
                                else
                                {
                                    cmd.CommandText = "INSERT INTO global_hero_matchups_enemy (game_version, game_type, league_tier, game_map, hero_level, hero, enemy, mirror, win_loss, games_played) VALUES (";
                                }
                                cmd.CommandText += "\"" + data.VersionSplit + "\"" + "," +
                                    "\"" + data.GameType_id + "\"" + "," +
                                    "\"" + data.Players[i].player_league_tier + "\"" + "," +
                                    "\"" + data.GameMap_id + "\"" + "," +
                                     "\"" + hero_level + "\"" + "," +
                                    "\"" + data.Players[i].Hero_id + "\"" + "," +
                                    "\"" + data.Players[j].Hero_id + "\"" + "," +
                                    "\"" + data.Players[j].Mirror + "\"" + "," +
                                    "\"" + win_loss + "\"" + "," +
                                    1 + ")";


                                cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                                    "game_version = VALUES(game_version), " +
                                    "game_type = VALUES(game_type), " +
                                    "league_tier = VALUES(league_tier), " +
                                    "game_map = VALUES(game_map), " +
                                    "hero_level = VALUES(hero_level), " +
                                    "hero = VALUES(hero), ";
                                if (data.Players[j].Team == data.Players[i].Team)
                                {
                                    cmd.CommandText += "ally = VALUES(ally), ";

                                }
                                else
                                {
                                    cmd.CommandText += "enemy = VALUES(enemy), ";
                                }
                                cmd.CommandText += "mirror = VALUES(mirror), ";
                                cmd.CommandText += "win_loss = VALUES(win_loss), " +
                                    "games_played = games_played + VALUES(games_played)";
                                //Console.WriteLine(cmd.CommandText);
                                var Reader = cmd.ExecuteReader();
                            }

                        }


                    }
                }
            }
        }
        private int insertTalentCombo(string hero, int level_one, int level_four, int level_seven, int level_ten, int level_thirteen, int level_sixteen, int level_twenty)
        {
            var comb_id = 0;

            using (var conn = new MySqlConnection(db_connect_string))
            {
                conn.Open();
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "INSERT INTO talent_combination_id (hero, level_one, level_four, level_seven, level_ten, level_thirteen, level_sixteen, level_twenty) VALUES (" +
                        hero + "," +
                        level_one + "," +
                        level_four + "," +
                        level_seven + "," +
                        level_ten + "," +
                        level_thirteen + "," +
                        level_sixteen + "," +
                        level_twenty +
                        ")";

                    var Reader = cmd.ExecuteReader();
                }


                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT talent_combination_id FROM heroesprofile.talent_combinations WHERE " +
                        "hero = " + hero +
                        " AND level_one = " + level_one +
                        " AND level_four = " + level_four +
                        " AND level_seven = " + level_seven +
                        " AND level_ten = " + level_ten +
                        " AND level_thirteen = " + level_thirteen +
                        " AND level_sixteen = " + level_sixteen +
                        " AND level_twenty = " + level_twenty;

                    var Reader = cmd.ExecuteReader();

                    while (Reader.Read())
                    {
                        comb_id = Reader.GetInt32("talent_combination_id");
                    }
                }
            }

            return comb_id;
        }

        private int getHeroCombID(string hero, int level_one, int level_four, int level_seven, int level_ten, int level_thirteen, int level_sixteen, int level_twenty)
        {
            var comb_id = 0;
            using (var conn = new MySqlConnection(db_connect_string))
            {
                conn.Open();
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT talent_combination_id FROM heroesprofile.talent_combinations WHERE " +
                        "hero = " + hero +
                        " AND level_one = " + level_one +
                        " AND level_four = " + level_four +
                        " AND level_seven = " + level_seven +
                        " AND level_ten = " + level_ten +
                        " AND level_thirteen = " + level_thirteen +
                        " AND level_sixteen = " + level_sixteen +
                        " AND level_twenty = " + level_twenty;

                    var Reader = cmd.ExecuteReader();

                    while (Reader.Read())
                    {
                        comb_id = Reader.GetInt32("talent_combination_id");
                    }
                    if (!Reader.HasRows)
                    {
                        comb_id = insertTalentCombo(hero, level_one, level_four, level_seven, level_ten, level_thirteen, level_sixteen, level_twenty);
                    }
                }
            }

            return comb_id;
        }
        private void updateGlobalTalentData(LambdaJson.ReplayData data, MySqlConnection conn)
        {
            for (var i = 0; i < data.Players.Length; i++)
            {
                var win_loss = 0;
                if (data.Players[i].Winner)
                {

                    win_loss = 1;
                }
                else
                {

                    win_loss = 0;
                }

                if (data.Players[i].Score != null)
                {

                    var hero_level = 0;

                    if (data.Players[i].HeroLevel < 5)
                    {
                        hero_level = 1;
                    }
                    else if (data.Players[i].HeroLevel >= 5 && data.Players[i].HeroLevel < 10)
                    {
                        hero_level = 5;
                    }
                    else if (data.Players[i].HeroLevel >= 10 && data.Players[i].HeroLevel < 15)
                    {
                        hero_level = 10;
                    }
                    else if (data.Players[i].HeroLevel >= 15 && data.Players[i].HeroLevel < 20)
                    {
                        hero_level = 15;
                    }
                    else if (data.Players[i].HeroLevel >= 20)
                    {
                        if (data.Players[i].MasteyTauntTier == 0)
                        {
                            hero_level = 20;
                        }
                        else if (data.Players[i].MasteyTauntTier == 1)
                        {
                            hero_level = 25;
                        }
                        else if (data.Players[i].MasteyTauntTier == 2)
                        {
                            hero_level = 40;
                        }
                        else if (data.Players[i].MasteyTauntTier == 3)
                        {
                            hero_level = 60;
                        }
                        else if (data.Players[i].MasteyTauntTier == 4)
                        {
                            hero_level = 80;
                        }
                        else if (data.Players[i].MasteyTauntTier == 5)
                        {
                            hero_level = 100;
                        }
                    }

                    using (var cmd = conn.CreateCommand())
                    {

                        cmd.CommandText = "INSERT INTO global_hero_talents (" +
                            "game_version, " +
                            "game_type, " +
                            "league_tier, " +
                            "hero_league_tier, " +
                            "role_league_tier, " +
                            "game_map, " +
                            "hero_level, " +
                            "hero, mirror," +
                            " win_loss, " +
                            "talent_combination_id, " +
                            "game_time, " +
                            "kills, " +
                            "assists, " +
                            "takedowns, " +
                            "deaths, " +
                            "highest_kill_streak, " +
                            "hero_damage, " +
                            "siege_damage, " +
                            "structure_damage, " +
                            "minion_damage, " +
                            "creep_damage, " +
                            "summon_damage, " +
                            "time_cc_enemy_heroes, " +
                            "healing, " +
                            "self_healing, " +
                            "damage_taken, " +
                            "experience_contribution, " +
                            "town_kills, " +
                            "time_spent_dead, " +
                            "merc_camp_captures, " +
                            "watch_tower_captures, " +
                            "protection_allies, " +
                            "silencing_enemies, " +
                            "rooting_enemies, " +
                            "stunning_enemies, " +
                            "clutch_heals, " +
                            "escapes, " +
                            "vengeance, " +
                            "outnumbered_deaths, " +
                            "teamfight_escapes, " +
                            "teamfight_healing, " +
                            "teamfight_damage_taken, " +
                            "teamfight_hero_damage, " +
                            "multikill, physical_damage, " +
                            "spell_damage, " +
                            "regen_globes, " +
                            "games_played" +
                            ") VALUES (" +
                        "\"" + data.VersionSplit + "\"" + ",";
                        cmd.CommandText += "\"" + data.GameType_id + "\"" + "," +
                        "\"" + 0 + "\"" + "," +
                        "\"" + 0 + "\"" + "," +
                        "\"" + 0 + "\"" + "," +
                        "\"" + data.GameMap_id + "\"" + "," +
                        "\"" + hero_level + "\"" + "," +
                        "\"" + data.Players[i].Hero_id + "\"" + "," +
                        "\"" + data.Players[i].Mirror + "\"" + "," +
                        "\"" + win_loss + "\"" + ",";



                        if (data.Players[i].Talents == null || data.Players[i].Talents[0] == null || data.Players[i].Talents[0] == "")
                        {
                            cmd.CommandText += getHeroCombID(
                                data.Players[i].Hero_id,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0) + ",";
                        }
                        else if (data.Players[i].Talents[1] == null || data.Players[i].Talents[1] == "")
                        {
                            cmd.CommandText += getHeroCombID(
                                data.Players[i].Hero_id,
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[0]]),
                                0,
                                0,
                                0,
                                0,
                                0,
                                0) + ",";

                        }
                        else if (data.Players[i].Talents[2] == null || data.Players[i].Talents[2] == "")
                        {
                            cmd.CommandText += getHeroCombID(
                                data.Players[i].Hero_id,
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[0]]),
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[1]]),
                                0,
                                0,
                                0,
                                0,
                                0) + ",";
                        }
                        else if (data.Players[i].Talents[3] == null || data.Players[i].Talents[3] == "")
                        {
                            cmd.CommandText += getHeroCombID(
                                data.Players[i].Hero_id,
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[0]]),
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[1]]),
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[2]]),
                                0,
                                0,
                                0,
                                0) + ",";
                        }
                        else if (data.Players[i].Talents[4] == null || data.Players[i].Talents[4] == "")
                        {
                            cmd.CommandText += getHeroCombID(
                                data.Players[i].Hero_id,
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[0]]),
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[1]]),
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[2]]),
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[3]]),
                                0,
                                0,
                                0) + ",";

                        }
                        else if (data.Players[i].Talents[5] == null || data.Players[i].Talents[5] == "")
                        {
                            cmd.CommandText += getHeroCombID(
                                data.Players[i].Hero_id,
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[0]]),
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[1]]),
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[2]]),
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[3]]),
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[4]]),
                                0,
                                0) + ",";
                        }
                        else if (data.Players[i].Talents[6] == null || data.Players[i].Talents[6] == "")
                        {
                            cmd.CommandText += getHeroCombID(
                                data.Players[i].Hero_id,
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[0]]),
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[1]]),
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[2]]),
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[3]]),
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[4]]),
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[5]]),
                                0) + ",";
                        }
                        else
                        {
                            cmd.CommandText += getHeroCombID(
                                data.Players[i].Hero_id,
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[0]]),
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[1]]),
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[2]]),
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[3]]),
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[4]]),
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[5]]),
                                Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[6]])) + ",";
                        }

                        cmd.CommandText += "\"" + data.Length.UtcDateTime.TimeOfDay.TotalSeconds + "\"" + "," +
                                        checkIfEmpty(data.Players[i].Score.SoloKills) + "," +
                                        checkIfEmpty(data.Players[i].Score.Assists) + "," +
                                        checkIfEmpty(data.Players[i].Score.Takedowns) + "," +
                                        checkIfEmpty(data.Players[i].Score.Deaths) + "," +
                                        checkIfEmpty(data.Players[i].Score.HighestKillStreak) + "," +
                                        checkIfEmpty(data.Players[i].Score.HeroDamage) + "," +
                                        checkIfEmpty(data.Players[i].Score.SiegeDamage) + "," +
                                        checkIfEmpty(data.Players[i].Score.StructureDamage) + "," +
                                        checkIfEmpty(data.Players[i].Score.MinionDamage) + "," +
                                        checkIfEmpty(data.Players[i].Score.CreepDamage) + "," +
                                        checkIfEmpty(data.Players[i].Score.SummonDamage) + "," +
                                        checkIfEmpty(Convert.ToInt64(data.Players[i].Score.TimeCCdEnemyHeroes_not_null.UtcDateTime.TimeOfDay.TotalSeconds)) + "," +
                                        checkIfEmpty(data.Players[i].Score.Healing) + "," +
                                        checkIfEmpty(data.Players[i].Score.SelfHealing) + "," +
                                        checkIfEmpty(data.Players[i].Score.DamageTaken) + "," +
                                        checkIfEmpty(data.Players[i].Score.ExperienceContribution) + "," +
                                        checkIfEmpty(data.Players[i].Score.TownKills) + "," +
                                        checkIfEmpty(Convert.ToInt64(data.Players[i].Score.TimeSpentDead.UtcDateTime.TimeOfDay.TotalSeconds)) + "," +
                                        checkIfEmpty(data.Players[i].Score.MercCampCaptures) + "," +
                                        checkIfEmpty(data.Players[i].Score.WatchTowerCaptures) + "," +
                                        checkIfEmpty(data.Players[i].Score.ProtectionGivenToAllies) + "," +
                                        checkIfEmpty(data.Players[i].Score.TimeSilencingEnemyHeroes) + "," +
                                        checkIfEmpty(data.Players[i].Score.TimeRootingEnemyHeroes) + "," +
                                        checkIfEmpty(data.Players[i].Score.TimeStunningEnemyHeroes) + "," +
                                        checkIfEmpty(data.Players[i].Score.ClutchHealsPerformed) + "," +
                                        checkIfEmpty(data.Players[i].Score.EscapesPerformed) + "," +
                                        checkIfEmpty(data.Players[i].Score.VengeancesPerformed) + "," +
                                        checkIfEmpty(data.Players[i].Score.OutnumberedDeaths) + "," +
                                        checkIfEmpty(data.Players[i].Score.TeamfightEscapesPerformed) + "," +
                                        checkIfEmpty(data.Players[i].Score.TeamfightHealingDone) + "," +
                                        checkIfEmpty(data.Players[i].Score.TeamfightDamageTaken) + "," +
                                        checkIfEmpty(data.Players[i].Score.TeamfightHeroDamage) + "," +

                                        checkIfEmpty(data.Players[i].Score.Multikill) + "," +
                                        checkIfEmpty(data.Players[i].Score.PhysicalDamage) + "," +
                                        checkIfEmpty(data.Players[i].Score.SpellDamage) + "," +
                                        checkIfEmpty(data.Players[i].Score.RegenGlobes) + "," +
                                        1 + ")";


                        cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                            "game_time = game_time + VALUES(game_time), " +
                            "kills = kills + VALUES(kills), " +
                            "assists = assists + VALUES(assists), " +
                            "takedowns = takedowns + VALUES(takedowns), " +
                            "deaths = deaths + VALUES(deaths), " +
                            "highest_kill_streak = highest_kill_streak + VALUES(highest_kill_streak), " +
                            "hero_damage = hero_damage + VALUES(hero_damage), " +
                            "siege_damage = siege_damage + VALUES(siege_damage), " +
                            "structure_damage = structure_damage + VALUES(structure_damage), " +
                            "minion_damage = minion_damage + VALUES(minion_damage), " +
                            "creep_damage = creep_damage + VALUES(creep_damage), " +
                            "summon_damage = summon_damage + VALUES(summon_damage), " +
                            "time_cc_enemy_heroes = time_cc_enemy_heroes + VALUES(time_cc_enemy_heroes), " +
                            "healing = healing + VALUES(healing), " +
                            "self_healing = self_healing + VALUES(self_healing), " +
                            "damage_taken = damage_taken + VALUES(damage_taken), " +
                            "experience_contribution = experience_contribution + VALUES(experience_contribution), " +
                            "town_kills = town_kills + VALUES(town_kills), " +
                            "time_spent_dead = time_spent_dead + VALUES(time_spent_dead), " +
                            "merc_camp_captures = merc_camp_captures + VALUES(merc_camp_captures), " +
                            "watch_tower_captures = watch_tower_captures + VALUES(watch_tower_captures), " +
                            "protection_allies = protection_allies + VALUES(protection_allies), " +
                            "silencing_enemies = silencing_enemies + VALUES(silencing_enemies), " +
                            "rooting_enemies = rooting_enemies + VALUES(rooting_enemies), " +
                            "stunning_enemies = stunning_enemies + VALUES(stunning_enemies), " +
                            "clutch_heals = clutch_heals + VALUES(clutch_heals), " +
                            "escapes = escapes + VALUES(escapes), " +
                            "vengeance = vengeance + VALUES(vengeance), " +
                            "outnumbered_deaths = outnumbered_deaths + VALUES(outnumbered_deaths), " +
                            "teamfight_escapes = teamfight_escapes + VALUES(teamfight_escapes), " +
                            "teamfight_healing = teamfight_healing + VALUES(teamfight_healing), " +
                            "teamfight_damage_taken = teamfight_damage_taken + VALUES(teamfight_damage_taken), " +
                            "teamfight_hero_damage = teamfight_hero_damage + VALUES(teamfight_hero_damage), " +
                            "multikill = multikill + VALUES(multikill), " +
                            "physical_damage = physical_damage + VALUES(physical_damage), " +
                            "spell_damage = spell_damage + VALUES(spell_damage), " +
                            "regen_globes = regen_globes + VALUES(regen_globes), " +
                            "games_played = games_played + VALUES(games_played)";
                        //Console.WriteLine(cmd.CommandText);
                        var Reader = cmd.ExecuteReader();
                    }
                }


            }

        }

        private void updateGameModeTotalGames(int season, LambdaJson.ReplayData data, MySqlConnection conn)
        {
            for (var i = 0; i < data.Players.Length; i++)
            {

                var wins = 0;
                var losses = 0;
                if (data.Players[i].Winner)
                {

                    wins = 1;
                }
                else
                {

                    losses = 1;
                }

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "INSERT INTO master_games_played_data (type_value, season, game_type, blizz_id, region, win, loss, games_played) VALUES (" +
                        "\"" + mmr_ids["player"] + "\"" + "," +
                        season + "," +
                        data.GameType_id + "," +
                        data.Players[i].BlizzId + "," +
                        data.Region + "," +
                        wins + "," +
                        losses + "," +
                            1 + ")";
                    cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                        "type_value = VALUES(type_value), " +
                        "season = VALUES(season), " +
                        "game_type = VALUES(game_type), " +
                        "blizz_id = VALUES(blizz_id), " +
                        "region = VALUES(region), " +
                        "win = win + VALUES(win), " +
                        "loss = loss + VALUES(loss), " +
                        "games_played = games_played + VALUES(games_played)";
                    //Console.WriteLine(cmd.CommandText);
                    var Reader = cmd.ExecuteReader();
                }


                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "INSERT INTO master_games_played_data (type_value, season, game_type, blizz_id, region, win, loss, games_played) VALUES (" +
                        "\"" + mmr_ids[role[data.Players[i].Hero]] + "\"" + "," +
                        season + "," +
                        data.GameType_id + "," +
                        data.Players[i].BlizzId + "," +
                        data.Region + "," +
                        wins + "," +
                        losses + "," +
                            1 + ")";
                    cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                        "type_value = VALUES(type_value), " +
                        "season = VALUES(season), " +
                        "game_type = VALUES(game_type), " +
                        "blizz_id = VALUES(blizz_id), " +
                        "region = VALUES(region), " +
                        "win = win + VALUES(win), " +
                        "loss = loss + VALUES(loss), " +
                        "games_played = games_played + VALUES(games_played)";
                    //Console.WriteLine(cmd.CommandText);
                    var Reader = cmd.ExecuteReader();
                }

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "INSERT INTO master_games_played_data (type_value, season, game_type, blizz_id, region, win, loss, games_played) VALUES (" +
                        "\"" + mmr_ids[data.Players[i].Hero] + "\"" + "," +
                        season + "," +
                        data.GameType_id + "," +
                        data.Players[i].BlizzId + "," +
                        data.Region + "," +
                        wins + "," +
                        losses + "," +
                            1 + ")";
                    cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                        "type_value = VALUES(type_value), " +
                        "season = VALUES(season), " +
                        "game_type = VALUES(game_type), " +
                        "blizz_id = VALUES(blizz_id), " +
                        "region = VALUES(region), " +
                        "win = win + VALUES(win), " +
                        "loss = loss + VALUES(loss), " +
                        "games_played = games_played + VALUES(games_played)";
                    //Console.WriteLine(cmd.CommandText);
                    var Reader = cmd.ExecuteReader();
                }
            }

        }
        private void updateGlobalTalentDataDetails(LambdaJson.ReplayData data, MySqlConnection conn)
        {
            for (var i = 0; i < data.Players.Length; i++)
            {
                for (var j = 0; j < data.Players.Length; j++)
                {
                    if (j != i)
                    {
                        if (data.Players[i].Hero == data.Players[j].Hero)
                        {
                            data.Players[i].Mirror = 1;
                            break;
                        }
                    }
                }
            }

            for (var i = 0; i < data.Players.Length; i++)
            {

                var win_loss = 0;
                if (data.Players[i].Winner)
                {

                    win_loss = 1;
                }
                else
                {

                    win_loss = 0;
                }

                if (data.Players[i].Talents != null)
                {
                    for (var t = 0; t < 7; t++)
                    {
                        var level = "";
                        if (t == 0)
                        {
                            level = "1";
                        }
                        else if (t == 1)
                        {
                            level = "4";

                        }
                        else if (t == 2)
                        {
                            level = "7";

                        }
                        else if (t == 3)
                        {
                            level = "10";

                        }
                        else if (t == 4)
                        {
                            level = "13";

                        }
                        else if (t == 5)
                        {
                            level = "16";

                        }
                        else if (t == 6)
                        {
                            level = "20";

                        }

                        if (data.Players[i].Score != null)
                        {
                            var hero_level = 0;

                            if (data.Players[i].HeroLevel < 5)
                            {
                                hero_level = 1;
                            }
                            else if (data.Players[i].HeroLevel >= 5 && data.Players[i].HeroLevel < 10)
                            {
                                hero_level = 5;
                            }
                            else if (data.Players[i].HeroLevel >= 10 && data.Players[i].HeroLevel < 15)
                            {
                                hero_level = 10;
                            }
                            else if (data.Players[i].HeroLevel >= 15 && data.Players[i].HeroLevel < 20)
                            {
                                hero_level = 15;
                            }
                            else if (data.Players[i].HeroLevel >= 20)
                            {
                                if (data.Players[i].MasteyTauntTier == 0)
                                {
                                    hero_level = 20;
                                }
                                else if (data.Players[i].MasteyTauntTier == 1)
                                {
                                    hero_level = 25;
                                }
                                else if (data.Players[i].MasteyTauntTier == 2)
                                {
                                    hero_level = 40;
                                }
                                else if (data.Players[i].MasteyTauntTier == 3)
                                {
                                    hero_level = 60;
                                }
                                else if (data.Players[i].MasteyTauntTier == 4)
                                {
                                    hero_level = 80;
                                }
                                else if (data.Players[i].MasteyTauntTier == 5)
                                {
                                    hero_level = 100;
                                }
                            }

                            using (var cmd = conn.CreateCommand())
                            {

                                cmd.CommandText = "INSERT INTO global_hero_talents_details (" +
                                    "game_version, " +
                                    "game_type, " +
                                    "league_tier, " +
                                    "hero_league_tier, " +
                                    "role_league_tier," +
                                    "game_map, " +
                                    "hero_level, " +
                                    "hero, " +
                                    "mirror, " +
                                    "win_loss, " +
                                    "level, " +
                                    "talent, " +
                                    "game_time, " +
                                    "kills, " +
                                    "assists, " +
                                    "takedowns, " +
                                    "deaths, " +
                                    "highest_kill_streak, " +
                                    "hero_damage, " +
                                    "siege_damage, " +
                                    "structure_damage, " +
                                    "minion_damage, " +
                                    "creep_damage, " +
                                    "summon_damage, " +
                                    "time_cc_enemy_heroes, " +
                                    "healing, " +
                                    "self_healing, " +
                                    "damage_taken, " +
                                    "experience_contribution, " +
                                    "town_kills, " +
                                    "time_spent_dead, " +
                                    "merc_camp_captures, " +
                                    "watch_tower_captures, " +
                                    "protection_allies, " +
                                    "silencing_enemies, " +
                                    "rooting_enemies, " +
                                    "stunning_enemies, " +
                                    "clutch_heals, " +
                                    "escapes, " +
                                    "vengeance, " +
                                    "outnumbered_deaths, " +
                                    "teamfight_escapes, " +
                                    "teamfight_healing, " +
                                    "teamfight_damage_taken, " +
                                    "teamfight_hero_damage, " +
                                    "multikill, physical_damage, " +
                                    "spell_damage, " +
                                    "regen_globes, " +
                                    "games_played) VALUES (" +
                                    "\"" + data.VersionSplit + "\"" + "," +
                                    data.GameType_id + "," +
                                    0 + "," +
                                    0 + "," +
                                    0 + "," +
                                    data.GameMap_id + "," +
                                    hero_level + "," +
                                    data.Players[i].Hero_id + "," +
                                    data.Players[i].Mirror + "," +
                                    "\"" + win_loss + "\"" + "," +
                                    level + ",";

                                if (t == 0)
                                {
                                    cmd.CommandText += Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[0]]) + ",";
                                }
                                else if (t == 1)
                                {
                                    cmd.CommandText += Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[1]]) + ",";
                                }
                                else if (t == 2)
                                {
                                    cmd.CommandText += Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[2]]) + ",";
                                }
                                else if (t == 3)
                                {
                                    cmd.CommandText += Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[3]]) + ",";
                                }
                                else if (t == 4)
                                {
                                    cmd.CommandText += Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[4]]) + ",";
                                }
                                else if (t == 5)
                                {
                                    cmd.CommandText += Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[5]]) + ",";
                                }
                                else if (t == 6)
                                {
                                    cmd.CommandText += Convert.ToInt32(talents[data.Players[i].Hero + "|" + data.Players[i].Talents[6]]) + ",";
                                }


                                cmd.CommandText += "\"" + data.Length.UtcDateTime.TimeOfDay.TotalSeconds + "\"" + "," +
                                        checkIfEmpty(data.Players[i].Score.SoloKills) + "," +
                                        checkIfEmpty(data.Players[i].Score.Assists) + "," +
                                        checkIfEmpty(data.Players[i].Score.Takedowns) + "," +
                                        checkIfEmpty(data.Players[i].Score.Deaths) + "," +
                                        checkIfEmpty(data.Players[i].Score.HighestKillStreak) + "," +
                                        checkIfEmpty(data.Players[i].Score.HeroDamage) + "," +
                                        checkIfEmpty(data.Players[i].Score.SiegeDamage) + "," +
                                        checkIfEmpty(data.Players[i].Score.StructureDamage) + "," +
                                        checkIfEmpty(data.Players[i].Score.MinionDamage) + "," +
                                        checkIfEmpty(data.Players[i].Score.CreepDamage) + "," +
                                        checkIfEmpty(data.Players[i].Score.SummonDamage) + "," +
                                        checkIfEmpty(Convert.ToInt64(data.Players[i].Score.TimeCCdEnemyHeroes_not_null.UtcDateTime.TimeOfDay.TotalSeconds)) + "," +
                                        checkIfEmpty(data.Players[i].Score.Healing) + "," +
                                        checkIfEmpty(data.Players[i].Score.SelfHealing) + "," +
                                        checkIfEmpty(data.Players[i].Score.DamageTaken) + "," +
                                        checkIfEmpty(data.Players[i].Score.ExperienceContribution) + "," +
                                        checkIfEmpty(data.Players[i].Score.TownKills) + "," +
                                        checkIfEmpty(Convert.ToInt64(data.Players[i].Score.TimeSpentDead.UtcDateTime.TimeOfDay.TotalSeconds)) + "," +
                                        checkIfEmpty(data.Players[i].Score.MercCampCaptures) + "," +
                                        checkIfEmpty(data.Players[i].Score.WatchTowerCaptures) + "," +
                                        checkIfEmpty(data.Players[i].Score.ProtectionGivenToAllies) + "," +
                                        checkIfEmpty(data.Players[i].Score.TimeSilencingEnemyHeroes) + "," +
                                        checkIfEmpty(data.Players[i].Score.TimeRootingEnemyHeroes) + "," +
                                        checkIfEmpty(data.Players[i].Score.TimeStunningEnemyHeroes) + "," +
                                        checkIfEmpty(data.Players[i].Score.ClutchHealsPerformed) + "," +
                                        checkIfEmpty(data.Players[i].Score.EscapesPerformed) + "," +
                                        checkIfEmpty(data.Players[i].Score.VengeancesPerformed) + "," +
                                        checkIfEmpty(data.Players[i].Score.OutnumberedDeaths) + "," +
                                        checkIfEmpty(data.Players[i].Score.TeamfightEscapesPerformed) + "," +
                                        checkIfEmpty(data.Players[i].Score.TeamfightHealingDone) + "," +
                                        checkIfEmpty(data.Players[i].Score.TeamfightDamageTaken) + "," +
                                        checkIfEmpty(data.Players[i].Score.TeamfightHeroDamage) + "," +

                                        checkIfEmpty(data.Players[i].Score.Multikill) + "," +
                                        checkIfEmpty(data.Players[i].Score.PhysicalDamage) + "," +
                                        checkIfEmpty(data.Players[i].Score.SpellDamage) + "," +
                                        checkIfEmpty(data.Players[i].Score.RegenGlobes) + "," +
                                        1 + ")";


                                cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                                    "game_time = game_time + VALUES(game_time), " +
                                    "kills = kills + VALUES(kills), " +
                                    "assists = assists + VALUES(assists), " +
                                    "takedowns = takedowns + VALUES(takedowns), " +
                                    "deaths = deaths + VALUES(deaths), " +
                                    "highest_kill_streak = highest_kill_streak + VALUES(highest_kill_streak), " +
                                    "hero_damage = hero_damage + VALUES(hero_damage), " +
                                    "siege_damage = siege_damage + VALUES(siege_damage), " +
                                    "structure_damage = structure_damage + VALUES(structure_damage), " +
                                    "minion_damage = minion_damage + VALUES(minion_damage), " +
                                    "creep_damage = creep_damage + VALUES(creep_damage), " +
                                    "summon_damage = summon_damage + VALUES(summon_damage), " +
                                    "time_cc_enemy_heroes = time_cc_enemy_heroes + VALUES(time_cc_enemy_heroes), " +
                                    "healing = healing + VALUES(healing), " +
                                    "self_healing = self_healing + VALUES(self_healing), " +
                                    "damage_taken = damage_taken + VALUES(damage_taken), " +
                                    "experience_contribution = experience_contribution + VALUES(experience_contribution), " +
                                    "town_kills = town_kills + VALUES(town_kills), " +
                                    "time_spent_dead = time_spent_dead + VALUES(time_spent_dead), " +
                                    "merc_camp_captures = merc_camp_captures + VALUES(merc_camp_captures), " +
                                    "watch_tower_captures = watch_tower_captures + VALUES(watch_tower_captures), " +
                                    "protection_allies = protection_allies + VALUES(protection_allies), " +
                                    "silencing_enemies = silencing_enemies + VALUES(silencing_enemies), " +
                                    "rooting_enemies = rooting_enemies + VALUES(rooting_enemies), " +
                                    "stunning_enemies = stunning_enemies + VALUES(stunning_enemies), " +
                                    "clutch_heals = clutch_heals + VALUES(clutch_heals), " +
                                    "escapes = escapes + VALUES(escapes), " +
                                    "vengeance = vengeance + VALUES(vengeance), " +
                                    "outnumbered_deaths = outnumbered_deaths + VALUES(outnumbered_deaths), " +
                                    "teamfight_escapes = teamfight_escapes + VALUES(teamfight_escapes), " +
                                    "teamfight_healing = teamfight_healing + VALUES(teamfight_healing), " +
                                    "teamfight_damage_taken = teamfight_damage_taken + VALUES(teamfight_damage_taken), " +
                                    "teamfight_hero_damage = teamfight_hero_damage + VALUES(teamfight_hero_damage), " +
                                    "multikill = multikill + VALUES(multikill), " +
                                    "physical_damage = physical_damage + VALUES(physical_damage), " +
                                    "spell_damage = spell_damage + VALUES(spell_damage), " +
                                    "regen_globes = regen_globes + VALUES(regen_globes), " +
                                    "games_played = games_played + VALUES(games_played)";
                                //Console.WriteLine(cmd.CommandText);
                                var Reader = cmd.ExecuteReader();
                            }
                        }
                    }
                }

            }

        }

        private string insertIntoTalentTable(string hero, string talent_name, MySqlConnection conn)
        {
            if (hero == "")
            {
                var split = Regex.Split(talent_name, @"(?<!^)(?=[A-Z])");
                if (heroes_alt.ContainsKey(split[0]))
                {
                    hero = heroes_alt[split[0]];

                }
                else
                {
                    hero = split[0];
                }

            }
            var id = "";
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "INSERT IGNORE INTO heroes_data_talents (hero_name, short_name, attribute_id, title, talent_name, description, status, hotkey, cooldown, mana_cost, sort, level, icon) VALUES " +
                    "(" +
                    "\"" + hero + "\"" + "," +
                    "\"" + "" + "\"" + "," +
                    "\"" + "" + "\"" + "," +
                    "\"" + "" + "\"" + "," +
                    "\"" + talent_name + "\"" + "," +
                    "\"" + "" + "\"" + "," +
                    "\"" + "" + "\"" + "," +
                    "\"" + "" + "\"" + "," +
                    "\"" + "" + "\"" + "," +
                    "\"" + "" + "\"" + "," +
                    "\"" + "" + "\"" + "," +
                    0 + "," +
                    "\"" + "" + "\"" +
                    ")";

                cmd.CommandTimeout = 0;



                var Reader = cmd.ExecuteReader();
            }


            using (var cmd = conn.CreateCommand())
            {

                cmd.CommandText = "SELECT talent_id from heroes_data_talents where hero_name = " + "\"" + hero + "\"" + " AND talent_name = " + "\"" + talent_name + "\"" + "  order by talent_id asc";
                cmd.CommandTimeout = 0;
                var Reader = cmd.ExecuteReader();

                while (Reader.Read())
                {
                    id = Reader.GetString("talent_id");
                }
            }

            return id;
        }

        private void insertUrlIntoReplayUrls(LambdaJson.ReplayData data, MySqlConnection conn)
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO replay_urls (replayID, game_date, game_type, url) VALUES (" +
                    "\"" + replayID + "\"" + "," +
                    "\"" + data.Date.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                    "\"" + data.GameType_id + "\"" + "," +
                    "\"" + replayURL + "\"" + ")";
                var Reader = cmd.ExecuteReader();
            }
        }

        private string saveToSeasonGameVersion(DateTime game_date, string game_version, MySqlConnection conn)
        {
            var season = "";



            foreach (var s in seasons.Keys)
            {
                if (game_date >= seasons[s][0] && game_date < seasons[s][1])
                {
                    season = s;
                }
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "INSERT IGNORE INTO season_game_versions (season, game_version) VALUES (" +
                    season + "," +
                    "\"" + game_version + "\"" + ")";
                Console.WriteLine(cmd.CommandText);
                var Reader = cmd.ExecuteReader();
            }


            return season;

        }

        private void insertIntoReplaysNotProcessed(string replayID, string parsedID, string region, string game_type, string game_length, string game_date, string game_map, string game_version, string size, string parsed_date, string url, string processed, string errorMessage)
        {
            using (var conn = new MySqlConnection(db_connect_string))
            {
                conn.Open();

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES (" +
                        "\"" + replayID + "\"" + "," +
                        "\"" + parsedID + "\"" + "," +
                        "\"" + region + "\"" + "," +
                        "\"" + game_type + "\"" + "," +
                        "\"" + game_length + "\"" + "," +
                        "\"" + game_date + "\"" + "," +
                        "\"" + game_map + "\"" + "," +
                        "\"" + game_version + "\"" + "," +
                        "\"" + size + "\"" + "," +
                        "\"" + parsed_date + "\"" + "," +
                        "\"" + "1" + "\"" + "," +
                        "\"" + url + "\"" + "," +
                        "\"" + processed + "\"" + "," +
                        "\"" + errorMessage + "\"" + ")";
                    cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                        "replayID = VALUES(replayID), " +
                        "parsedID = VALUES(parsedID), " +
                        "region = VALUES(region)," +
                        "game_type = VALUES(game_type)," +
                        "game_length = VALUES(game_length)," +
                        "game_date = VALUES(game_date)," +
                        "game_version = VALUES(game_version)," +
                        "size = VALUES(size)," +
                        "date_parsed = VALUES(date_parsed)," +
                        "count_parsed = count_parsed + VALUES(count_parsed)," +
                        "url = VALUES(url)," +
                        "processed = VALUES(processed)," +
                        "failure_status = VALUES(failure_status)";
                    var Reader = cmd.ExecuteReader();
                }
            }
        }

        public ParseStormReplay(Uri replayURL)
        {
            var globalJson = "";
            var httpWebRequest = (HttpWebRequest)WebRequest.Create("https://a73l75cbzg.execute-api.eu-west-1.amazonaws.com/default/parse-hots");

            httpWebRequest.Method = "POST";
            httpWebRequest.Timeout = 1000000;

            using (var streamWriter = new StreamWriter(httpWebRequest.GetRequestStream()))
            {
                var json = new JavaScriptSerializer().Serialize(new
                {
                    //input = "http://hotsapi.s3-website-eu-west-1.amazonaws.com/c5a49c21-d3d0-c8d9-c904-b3d09feea5e9.StormReplay",
                    input = replayURL,
                    access = "", //Need to pull from config file or ENV
                    secret = "" //Need to pull from config file or ENV
                });

                streamWriter.Write(json);
            }

            var httpResponse = (HttpWebResponse)httpWebRequest.GetResponse();
            var result = "";
            using (var streamReader = new StreamReader(httpResponse.GetResponseStream()))
            {
                result = streamReader.ReadToEnd();
                Console.WriteLine(result);
                globalJson = result;
            }

            var data = LambdaJson.ReplayData.FromJson(globalJson);



        }



    }

}
