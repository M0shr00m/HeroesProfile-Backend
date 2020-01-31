using System;
using System.Collections.Generic;
using System.Net;
using System.IO;
using System.Linq;
using MySql.Data.MySqlClient;
using System.Text.RegularExpressions;
using HeroesProfile_Backend.Models;
using Newtonsoft.Json;

namespace HeroesProfile_Backend
{
    public class ParseStormReplayService
    {
        private readonly ApiSettings _apiSettings;
        private readonly DbSettings _dbSettings;
        private readonly string _connectionString;

        public ParseStormReplayService(ApiSettings apiSettings, DbSettings dbSettings)
        {
            _dbSettings = dbSettings;
            _apiSettings = apiSettings;
            _connectionString = ConnectionStringBuilder.BuildConnectionString(_dbSettings);
        }

        public ParsedStormReplay ParseStormReplay(long replayId, Uri replayUrl, HotsApiJSON.ReplayData hotsapiData, Dictionary<string, string> maps, 
                                                  Dictionary<string, string> mapsTranslations, Dictionary<string, string> gameTypes, Dictionary<string, string> talents, 
                                                  Dictionary<string, string> seasonsGameVersions, Dictionary<string, string> mmrIds, Dictionary<string, DateTime[]> seasons,
                                                  Dictionary<string, string> heroes, Dictionary<string, string> heroesTranslations, Dictionary<string, string> mapsShort,
                                                  Dictionary<string, string> mmrs, Dictionary<string, string> roles, Dictionary<string, string> heroesAttr)
        {
            var parsedReplay = new ParsedStormReplay()
            {
                    ReplayId = replayId,
                    ReplayUrl = replayUrl,
                    Maps = maps,
                    MapsTranslations = mapsTranslations,
                    GameTypes = gameTypes,
                    Talents = talents,
                    SeasonsGameVersions = seasonsGameVersions,
                    Seasons = seasons,
                    HeroesTranslations = heroesTranslations,
                    MapsShort = mapsShort,
                    Heroes = heroes,
                    MmrIds = mmrs,
                    Role = roles,
                    HeroesAttr = heroesAttr
            };
            try
            {
                var globalJson = "";
                var httpWebRequest = (HttpWebRequest) WebRequest.Create(_apiSettings.lambda_parser_endpoint_url);

                httpWebRequest.Method = "POST";
                httpWebRequest.Timeout = 1000000;

                using (var streamWriter = new StreamWriter(httpWebRequest.GetRequestStream()))
                {
                    var json = JsonConvert.SerializeObject(new
                    {
                            //input = "http://hotsapi.s3-website-eu-west-1.amazonaws.com/c5a49c21-d3d0-c8d9-c904-b3d09feea5e9.StormReplay",
                            input = replayUrl,
                            access = _apiSettings.lambda_parser_endpoint_access,
                            secret = _apiSettings.lambda_parser_endpoint_secret 
                    });

                    streamWriter.Write(json);
                }

                var httpResponse = (HttpWebResponse) httpWebRequest.GetResponse();
                var result = "";
                using (var streamReader = new StreamReader(httpResponse.GetResponseStream()))
                {
                    result = streamReader.ReadToEnd();
                    globalJson = result;
                }


                if (Regex.Match(result, "Error parsing replay: UnexpectedResult").Success)
                {
                    using var conn = new MySqlConnection(_connectionString);
                    conn.Open();
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText =
                            "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                            replayId + "," +
                            "\"" + "NULL" + "\"" + "," +
                            "\"" + hotsapiData.Region + "\"" + "," +
                            "\"" + hotsapiData.GameType + "\"" + "," +
                            "\"" + hotsapiData.GameLength + "\"" + "," +
                            "\"" + hotsapiData.GameDate.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                            "\"" + hotsapiData.GameMap + "\"" + "," +
                            "\"" + hotsapiData.GameVersion + "\"" + "," +
                            "\"" + hotsapiData.Size + "\"" + "," +
                            "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                            1 + "," +
                            "\"" + replayUrl + "\"" + "," +
                            "\"" + hotsapiData.Processed + "\"" + "," +
                            "\"" + "Error parsing replay: UnexpectedResult" + "\"" + ")";
                    var reader = cmd.ExecuteReader();
                }
                else if (Regex.Match(result, "Error parsing replay: SuccessReplayDetail").Success)
                {
                    using var conn = new MySqlConnection(_connectionString);
                    conn.Open();
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText =
                            "INSERT IGNORE INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                            replayId + "," +
                            "\"" + "NULL" + "\"" + "," +
                            "\"" + hotsapiData.Region + "\"" + "," +
                            "\"" + hotsapiData.GameType + "\"" + "," +
                            "\"" + hotsapiData.GameLength + "\"" + "," +
                            "\"" + hotsapiData.GameDate.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                            "\"" + hotsapiData.GameMap + "\"" + "," +
                            "\"" + hotsapiData.GameVersion + "\"" + "," +
                            "\"" + hotsapiData.Size + "\"" + "," +
                            "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                            1 + "," +
                            "\"" + replayUrl + "\"" + "," +
                            "\"" + hotsapiData.Processed + "\"" + "," +
                            "\"" + "Error parsing replay: SuccessReplayDetail" + "\"" + ")";
                    var reader = cmd.ExecuteReader();
                }
                else if (Regex.Match(result, "Error parsing replay: ParserException").Success)
                {
                    using var conn = new MySqlConnection(_connectionString);
                    conn.Open();
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText =
                            "INSERT IGNORE INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                            replayId + "," +
                            "\"" + "NULL" + "\"" + "," +
                            "\"" + hotsapiData.Region + "\"" + "," +
                            "\"" + hotsapiData.GameType + "\"" + "," +
                            "\"" + hotsapiData.GameLength + "\"" + "," +
                            "\"" + hotsapiData.GameDate.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                            "\"" + hotsapiData.GameMap + "\"" + "," +
                            "\"" + hotsapiData.GameVersion + "\"" + "," +
                            "\"" + hotsapiData.Size + "\"" + "," +
                            "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                            1 + "," +
                            "\"" + replayUrl + "\"" + "," +
                            "\"" + hotsapiData.Processed + "\"" + "," +
                            "\"" + "Error parsing replay: ParserException" + "\"" + ")";
                    var reader = cmd.ExecuteReader();
                }
                else
                {
                    var data = LambdaJson.ReplayData.FromJson(globalJson);
                    parsedReplay.OverallData = data;
                    //if (data.Mode != "Brawl")
                    //{
                    if (data.Version != null)
                    {
                        var version = new Version(data.Version);

                        foreach (var player in data.Players)
                        {
                            if (heroesTranslations.ContainsKey(player.Hero.ToLower()))
                            {
                                player.Hero = heroesTranslations[player.Hero.ToLower()];
                            }

                            player.Score.TimeCCdEnemyHeroes_not_null = DateTimeOffset.TryParse(player.Score.TimeCCdEnemyHeroes, out var dateValue) ? dateValue : DateTimeOffset.Parse("00:00:00");
                        }

                        if (mapsTranslations.ContainsKey(data.Map))
                        {
                            data.Map = mapsTranslations[data.Map];
                        }
                        else
                        {
                            if (mapsShort.ContainsKey(data.MapShort))
                            {
                                data.Map = mapsShort[data.MapShort];
                            }
                        }

                        //if (data.Mode != "Brawl" && data.Mode != null && data.Date != null && data.Length != null && data.Region != null)
                        if (data.Mode != null && data.Date != null && data.Length != null && data.Region != null)
                        {
                            var orderedPlayers = new LambdaJson.Player[10];

                            var team1 = 0;
                            var team2 = 5;
                            foreach (var player in data.Players)
                            {
                                switch (player.Team)
                                {
                                    case 0:
                                        orderedPlayers[team1] = player;
                                        team1++;
                                        break;
                                    case 1:
                                        orderedPlayers[team2] = player;
                                        team2++;
                                        break;
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

                            if (gameTypes.ContainsKey(data.Mode))
                            {
                                data.GameType_id = gameTypes[data.Mode];

                            }
                            else
                            {
                                badGameType = true;
                            }

                            if (!badMap && !badGameType)
                            {
                                using var conn = new MySqlConnection(_connectionString);
                                conn.Open();
                                using var cmd = conn.CreateCommand();
                                if (data.Mode != "Brawl")
                                {
                                    cmd.CommandText = "SELECT replayID FROM replay where replayID = " + replayId;
                                }
                                else
                                {
                                    cmd.CommandText = "SELECT replayID FROM heroesprofile_brawl.replay where replayID = " + replayId;
                                }

                                var reader = cmd.ExecuteReader();

                                if (reader.HasRows)
                                {
                                    parsedReplay.Dupe = true;
                                }
                            }
                            else
                            {
                                using var conn = new MySqlConnection(_connectionString);
                                conn.Open();
                                using var cmd = conn.CreateCommand();
                                cmd.CommandText =
                                        "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                                        replayId + "," +
                                        "\"" + "NULL" + "\"" + "," +
                                        "\"" + hotsapiData.Region + "\"" + "," +
                                        "\"" + hotsapiData.GameType + "\"" + "," +
                                        "\"" + hotsapiData.GameLength + "\"" + "," +
                                        "\"" + hotsapiData.GameDate.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                        "\"" + hotsapiData.GameMap + "\"" + "," +
                                        "\"" + hotsapiData.GameVersion + "\"" + "," +
                                        "\"" + hotsapiData.Size + "\"" + "," +
                                        "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                        1 + "," +
                                        "\"" + replayUrl + "\"" + "," +
                                        "\"" + hotsapiData.Processed + "\"" + "," +
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
                                var reader = cmd.ExecuteReader();
                            }

                        }
                        else
                        {
                            using var conn = new MySqlConnection(_connectionString);
                            conn.Open();
                            using var cmd = conn.CreateCommand();
                            cmd.CommandText =
                                    "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                                    replayId + "," +
                                    "\"" + "NULL" + "\"" + "," +
                                    "\"" + hotsapiData.Region + "\"" + "," +
                                    "\"" + hotsapiData.GameType + "\"" + "," +
                                    "\"" + hotsapiData.GameLength + "\"" + "," +
                                    "\"" + hotsapiData.GameDate.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                    "\"" + hotsapiData.GameMap + "\"" + "," +
                                    "\"" + hotsapiData.GameVersion + "\"" + "," +
                                    "\"" + hotsapiData.Size + "\"" + "," +
                                    "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                    1 + "," +
                                    "\"" + replayUrl + "\"" + "," +
                                    "\"" + hotsapiData.Processed + "\"" + "," +
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
                            var reader = cmd.ExecuteReader();
                        }
                    }
                    else
                    {
                        using var conn = new MySqlConnection(_connectionString);
                        conn.Open();
                        using var cmd = conn.CreateCommand();
                        cmd.CommandText =
                                "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                                replayId + "," +
                                "\"" + "NULL" + "\"" + "," +
                                "\"" + hotsapiData.Region + "\"" + "," +
                                "\"" + hotsapiData.GameType + "\"" + "," +
                                "\"" + hotsapiData.GameLength + "\"" + "," +
                                "\"" + hotsapiData.GameDate.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                "\"" + hotsapiData.GameMap + "\"" + "," +
                                "\"" + "" + "\"" + "," +
                                "\"" + hotsapiData.Size + "\"" + "," +
                                "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                1 + "," +
                                "\"" + replayUrl + "\"" + "," +
                                "\"" + hotsapiData.Processed + "\"" + "," +
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
                        var reader = cmd.ExecuteReader();
                    }
                }
            }
            catch (Exception e)
            {
                using var conn = new MySqlConnection(_connectionString);
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText =
                        "INSERT IGNORE INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                        replayId + "," +
                        "\"" + "NULL" + "\"" + "," +
                        "\"" + 0 + "\"" + "," +
                        "\"" + "NULL" + "\"" + "," +
                        "\"" + "NULL" + "\"" + "," +
                        "\"" + hotsapiData.GameDate.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                        "\"" + "NULL" + "\"" + "," +
                        "\"" + "NULL" + "\"" + "," +
                        "\"" + "NULL" + "\"" + "," +
                        "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                        1 + "," +
                        "\"" + replayUrl + "\"" + "," +
                        "\"" + "NULL" + "\"" + "," +
                        "\"" + e + "\"" + ")";
                var reader = cmd.ExecuteReader();
            }

            return parsedReplay;
        }

        public ParsedStormReplay ParseStormReplay(long replayId, Uri replayUrl, ReplaysNotProcessed notProcessedReplays, Dictionary<string, string> maps, Dictionary<string, string> mapsTranslations,
                                Dictionary<string, string> gameTypes, Dictionary<string, string> talents, Dictionary<string, string> seasonsGameVersions, Dictionary<string, string> mmrIds,
                                Dictionary<string, DateTime[]> seasons, Dictionary<string, string> heroes, Dictionary<string, string> heroesTranslations, Dictionary<string, string> mapsShort,
                                Dictionary<string, string> mmrs, Dictionary<string, string> roles, Dictionary<string, string> heroesAttr)
        {
            var parsedReplay = new ParsedStormReplay
            {
                    ReplayId = replayId,
                    ReplayUrl = replayUrl,
                    Maps = maps,
                    MapsTranslations = mapsTranslations,
                    GameTypes = gameTypes,
                    Talents = talents,
                    SeasonsGameVersions = seasonsGameVersions,
                    Seasons = seasons,
                    HeroesTranslations = heroesTranslations,
                    MapsShort = mapsShort,
                    Heroes = heroes,
                    MmrIds = mmrs,
                    Role = roles,
                    HeroesAttr = heroesAttr
            };

            try
            {
                var globalJson = "";
                var httpWebRequest = (HttpWebRequest) WebRequest.Create(_apiSettings.lambda_parser_endpoint_url);

                httpWebRequest.Method = "POST";
                httpWebRequest.Timeout = 1000000;

                using (var streamWriter = new StreamWriter(httpWebRequest.GetRequestStream()))
                {
                    var json = JsonConvert.SerializeObject(new
                    {
                            //input = "http://hotsapi.s3-website-eu-west-1.amazonaws.com/c5a49c21-d3d0-c8d9-c904-b3d09feea5e9.StormReplay",
                            input = replayUrl,
                            access = _apiSettings.lambda_parser_endpoint_access,
                            secret = _apiSettings.lambda_parser_endpoint_secret 
                    });

                    streamWriter.Write(json);
                }

                var httpResponse = (HttpWebResponse) httpWebRequest.GetResponse();
                var result = "";
                using (var streamReader = new StreamReader(httpResponse.GetResponseStream()))
                {
                    result = streamReader.ReadToEnd();
                    //Console.WriteLine(result);
                    globalJson = result;
                }


                if (Regex.Match(result, "Error parsing replay: UnexpectedResult").Success)
                {
                    using var conn = new MySqlConnection(_connectionString);
                    conn.Open();
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText =
                            "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                            replayId + "," +
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
                            "\"" + replayUrl + "\"" + "," +
                            "\"" + "NULL" + "\"" + "," +
                            "\"" + "Error parsing replay: UnexpectedResult" + "\"" + ")";
                    cmd.CommandText += " ON DUPLICATE KEY UPDATE count_parsed = count_parsed + 1";
                    var reader = cmd.ExecuteReader();
                }

                else if (Regex.Match(result, "Error parsing replay: SuccessReplayDetail").Success)
                {
                    using var conn = new MySqlConnection(_connectionString);
                    conn.Open();
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText =
                            "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                            replayId + "," +
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
                            "\"" + replayUrl + "\"" + "," +
                            "\"" + "NULL" + "\"" + "," +
                            "\"" + "Error parsing replay: SuccessReplayDetail" + "\"" + ")";
                    cmd.CommandText += " ON DUPLICATE KEY UPDATE count_parsed = count_parsed + 1";
                    var reader = cmd.ExecuteReader();
                }
                else if (Regex.Match(result, "Error parsing replay: ParserException").Success)
                {
                    using var conn = new MySqlConnection(_connectionString);
                    conn.Open();
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText =
                            "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                            replayId + "," +
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
                            "\"" + replayUrl + "\"" + "," +
                            "\"" + "NULL" + "\"" + "," +
                            "\"" + "Error parsing replay: ParserException" + "\"" + ")";
                    cmd.CommandText += " ON DUPLICATE KEY UPDATE count_parsed = count_parsed + 1";
                    var reader = cmd.ExecuteReader();
                }

                else if (Regex.Match(result, "Error parsing replay: ParserException").Success)
                {
                    using var conn = new MySqlConnection(_connectionString);
                    conn.Open();
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText =
                            "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                            replayId + "," +
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
                            "\"" + replayUrl + "\"" + "," +
                            "\"" + "NULL" + "\"" + "," +
                            "\"" + "Error parsing replay: ParserException" + "\"" + ")";
                    cmd.CommandText += " ON DUPLICATE KEY UPDATE count_parsed = count_parsed + 1, failure_status = " + "\"" + result + "\"";
                    var reader = cmd.ExecuteReader();
                }

                else
                {
                    var data = LambdaJson.ReplayData.FromJson(globalJson);
                    parsedReplay.OverallData = data;
                    //if (data.Mode != "Brawl")
                    // {
                    if (data.Version != null)
                    {
                        var version = new Version(data.Version);

                        foreach (var player in data.Players)
                        {
                            if (heroesTranslations.ContainsKey(player.Hero.ToLower()))
                            {
                                player.Hero = heroesTranslations[player.Hero.ToLower()];
                            }

                            player.Score.TimeCCdEnemyHeroes_not_null = DateTimeOffset.TryParse(player.Score.TimeCCdEnemyHeroes, out var dateValue) ? dateValue : DateTimeOffset.Parse("00:00:00");
                        }

                        if (mapsTranslations.ContainsKey(data.Map))
                        {
                            data.Map = mapsTranslations[data.Map];
                        }
                        else
                        {
                            if (mapsShort.ContainsKey(data.MapShort))
                            {
                                data.Map = mapsShort[data.MapShort];
                            }
                        }

                        // if (data.Mode != "Brawl" && data.Mode != null && data.Date != null && data.Length != null && data.Region != null)
                        if (data.Mode != null && data.Date != null && data.Length != null && data.Region != null)
                        {


                            var orderedPlayers = new LambdaJson.Player[10];

                            var team1 = 0;
                            var team2 = 5;
                            foreach (var player in data.Players)
                            {
                                switch (player.Team)
                                {
                                    case 0:
                                        orderedPlayers[team1] = player;
                                        team1++;
                                        break;
                                    case 1:
                                        orderedPlayers[team2] = player;
                                        team2++;
                                        break;
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

                            if (gameTypes.ContainsKey(data.Mode))
                            {
                                data.GameType_id = gameTypes[data.Mode];

                            }
                            else
                            {
                                badGameType = true;
                            }

                            if (!badMap && !badGameType)
                            {
                                using var conn = new MySqlConnection(_connectionString);
                                conn.Open();
                                using var cmd = conn.CreateCommand();
                                if (data.Mode != "Brawl")
                                {
                                    cmd.CommandText = "SELECT replayID FROM replay where replayID = " + replayId;
                                }
                                else
                                {
                                    cmd.CommandText = "SELECT replayID FROM heroesprofile_brawl.replay where replayID = " + replayId;
                                }

                                var reader = cmd.ExecuteReader();

                                if (reader.HasRows)
                                {
                                    parsedReplay.Dupe = true;
                                }
                            }
                            else
                            {
                                using var conn = new MySqlConnection(_connectionString);
                                conn.Open();
                                using var cmd = conn.CreateCommand();
                                cmd.CommandText =
                                        "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                                        replayId + "," +
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
                                        "\"" + replayUrl + "\"" + "," +
                                        "\"" + "NULL" + "\"" + "," +
                                        "\"" + "Map or Game Type Bad" + "\"" + ")";
                                cmd.CommandText += " ON DUPLICATE KEY UPDATE count_parsed = count_parsed + 1";
                                var reader = cmd.ExecuteReader();
                            }

                        }
                        else
                        {
                            using var conn = new MySqlConnection(_connectionString);
                            conn.Open();
                            using var cmd = conn.CreateCommand();
                            cmd.CommandText =
                                    "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                                    replayId + "," +
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
                                    "\"" + replayUrl + "\"" + "," +
                                    "\"" + "NULL" + "\"" + "," +
                                    "\"" + "Map or Game Type Bad" + "\"" + ")";
                            cmd.CommandText += " ON DUPLICATE KEY UPDATE count_parsed = count_parsed + 1";
                            var reader = cmd.ExecuteReader();
                        }
                    }
                    else
                    {
                        using var conn = new MySqlConnection(_connectionString);
                        conn.Open();
                        using var cmd = conn.CreateCommand();
                        cmd.CommandText =
                                "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                                replayId + "," +
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
                                "\"" + replayUrl + "\"" + "," +
                                "\"" + "NULL" + "\"" + "," +
                                "\"" + "Game Version Null" + "\"" + ")";
                        cmd.CommandText += " ON DUPLICATE KEY UPDATE count_parsed = count_parsed + 1";
                        var reader = cmd.ExecuteReader();
                    }


                    //}

                }

                if (!parsedReplay.Dupe) return parsedReplay;
                {
                    using var conn = new MySqlConnection(_connectionString);
                    conn.Open();
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = "DELETE FROM replays_not_processed WHERE replayID = " + replayId;
                    var reader = cmd.ExecuteReader();
                }
            }
            catch (Exception e)
            {
                using var conn = new MySqlConnection(_connectionString);
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText =
                        "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                        replayId + "," +
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
                        "\"" + replayUrl + "\"" + "," +
                        "\"" + "NULL" + "\"" + "," +
                        "\"" + e + "\"" + ")";
                cmd.CommandText += " ON DUPLICATE KEY UPDATE count_parsed = count_parsed + 1";
                var reader = cmd.ExecuteReader();
            }

            return parsedReplay;
        }

        public void SaveReplayData(ParsedStormReplay parsedStormReplay, bool isBrawl)
        {
            try
            {
                if (isBrawl)
                {
                    parsedStormReplay.Data.GameType_id = "-1";
                }
                if (parsedStormReplay.OverallData.Players == null) return;
                using var conn = new MySqlConnection(_connectionString);
                conn.Open();
                var badHeroName = false;
                var badTalentName = false;
                if (parsedStormReplay.OverallData.Players.Length != 10) return;
                foreach (var player in parsedStormReplay.OverallData.Players)
                {
                    if (player.Hero == null)
                    {
                        if (player.Talents != null)
                        {
                            var split = Regex.Split(player.Talents[0], @"(?<!^)(?=[A-Z])");

                            if (parsedStormReplay.Heroes.ContainsKey(split[0]))
                            {
                                player.Hero = split[0];
                                player.Hero_id = parsedStormReplay.Heroes[split[0]];
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
                        if (!parsedStormReplay.Heroes.ContainsKey(player.Hero))
                        {
                            Console.WriteLine(player.Hero);
                            Console.WriteLine(player.Talents[0]);
                            badHeroName = true;
                            break;
                        }
                        else
                        {
                            player.Hero_id = parsedStormReplay.Heroes[player.Hero];
                        }
                    }


                    if (player.Talents == null) continue;
                    foreach (var talent in player.Talents)
                    {
                        if (!parsedStormReplay.Talents.ContainsKey(player.Hero + "|" + talent))
                        {
                            parsedStormReplay.Talents.Add(player.Hero + "|" + talent, InsertIntoTalentTable(player.Hero, talent, parsedStormReplay.HeroesAlt, conn));
                        }
                    }
                }

                var teamOneLevelTenTime = DateTimeOffset.Now;
                var teamTwoLevelTenTime = DateTimeOffset.Now;

                for (var teams = 0; teams < parsedStormReplay.OverallData.TeamExperience.Length; teams++)
                {
                    for (var teamTimeSplit = 0; teamTimeSplit < parsedStormReplay.OverallData.TeamExperience[teams].Length; teamTimeSplit++)
                    {
                        if (!(parsedStormReplay.OverallData.TeamExperience[teams][teamTimeSplit].TeamLevel >= 10)) continue;
                        if (teams == 0)
                        {
                            teamOneLevelTenTime = parsedStormReplay.OverallData.TeamExperience[teams][teamTimeSplit].TimeSpan;
                            break;

                        }
                        else
                        {
                            teamTwoLevelTenTime = parsedStormReplay.OverallData.TeamExperience[teams][teamTimeSplit].TimeSpan;
                            break;

                        }
                    }
                }

                var teamOneFirstToTen = 0;
                var teamTwoFirstToTen = 0;
                if (teamOneLevelTenTime < teamTwoLevelTenTime)
                {
                    teamOneFirstToTen = 1;
                }
                else
                {
                    teamTwoFirstToTen = 1;
                }

                foreach (var player in parsedStormReplay.OverallData.Players)
                {
                    player.Score.FirstToTen = player.Team == 0 ? teamOneFirstToTen : teamTwoFirstToTen;
                }


                if (!badHeroName)
                {

                    foreach (var player in parsedStormReplay.OverallData.Players)
                    {
                        //Console.WriteLine("Adding Battletag to battletag table for: " + data.Players.Battletag);

                        using (var cmd = conn.CreateCommand())
                        {
                            cmd.CommandText = "INSERT INTO battletags (blizz_id, battletag, region, account_level, latest_game) VALUES " +
                                              "(" + player.BlizzId + "," +
                                              "\"" + player.BattletagName + "#" + player.BattletagId + "\"" + "," +
                                              parsedStormReplay.OverallData.Region + "," +
                                              player.AccountLevel + "," +
                                              "\"" + parsedStormReplay.OverallData.Date.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + ")";

                            cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                                               "account_level = IF(VALUES(account_level) > account_level, VALUES(account_level), account_level), " +
                                               "latest_game = IF(VALUES(latest_game) > latest_game, VALUES(latest_game), latest_game) ";
                            cmd.CommandTimeout = 0;



                            var reader = cmd.ExecuteReader();
                        }
                        //Console.WriteLine("Getting player_id for: " + data.Players.Battletag);


                        using (var cmd = conn.CreateCommand())
                        {
                            cmd.CommandText = "SELECT player_id from battletags where battletag = " + "\"" + player.BattletagName + "#" + player.BattletagId + "\"" + " AND region = " + parsedStormReplay.OverallData.Region;
                            cmd.CommandTimeout = 0;
                            var reader = cmd.ExecuteReader();

                            while (reader.Read())
                            {
                                player.battletag_table_id = reader.GetString("player_id");
                            }
                        }
                    }



                    //Console.WriteLine("Saving Replay Information for:" + data.Id);

                    using (var cmd = conn.CreateCommand())
                    {
                        if (isBrawl)
                        {
                            cmd.CommandText = "INSERT INTO heroesprofile_brawl.replay (replayID, game_date, game_length, game_map, game_version, region, date_added) VALUES(" +
                                              parsedStormReplay.ReplayId + "," +
                                              "\"" + parsedStormReplay.OverallData.Date.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                              parsedStormReplay.OverallData.Length.UtcDateTime.TimeOfDay.TotalSeconds + "," +
                                              parsedStormReplay.Maps[parsedStormReplay.OverallData.Map] + "," +
                                              "\"" + parsedStormReplay.OverallData.Version + "\"" + "," +
                                              parsedStormReplay.OverallData.Region + "," +
                                              "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + ")";
                        }
                        else
                        {

                            cmd.CommandText = "INSERT INTO replay (replayID, parsed_id, game_type, game_date, game_length, game_map, game_version, region, date_added) VALUES(" +
                                              parsedStormReplay.ReplayId + "," +
                                              "NULL" + "," +
                                              parsedStormReplay.GameTypes[parsedStormReplay.OverallData.Mode] + "," +
                                              "\"" + parsedStormReplay.OverallData.Date.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                                              parsedStormReplay.OverallData.Length.UtcDateTime.TimeOfDay.TotalSeconds + "," +
                                              parsedStormReplay.Maps[parsedStormReplay.OverallData.Map] + "," +
                                              "\"" + parsedStormReplay.OverallData.Version + "\"" + "," +
                                              parsedStormReplay.OverallData.Region + "," +
                                              "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + ")";
                        }

                        cmd.CommandTimeout = 0;
                        //Console.WriteLine(cmd.CommandText);
                        var reader = cmd.ExecuteReader();
                    }


                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "DELETE FROM replays_not_processed WHERE replayID = " + parsedStormReplay.ReplayId;
                        var reader = cmd.ExecuteReader();
                    }


                    //// Add back in when running MMR Recalcs
                    /*
                    if (game_types[data.Mode] == "5")
                    {
                        using var cmd = conn.CreateCommand();
                        cmd.CommandText = "INSERT IGNORE INTO replays_ran_mmr (replayID, game_date) VALUES(" +
                                          replayID + "," +

                                          "\"" + data.Date.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + ")";
                        cmd.CommandTimeout = 0;
                        // Console.WriteLine(cmd.CommandText);
                        var Reader = cmd.ExecuteReader();
                    }
                    */

                    if (!isBrawl)
                    {
                        foreach (var ban in parsedStormReplay.OverallData.Bans)
                        {
                            for (var j = 0; j < ban.Length; j++)
                            {
                                if (ban[j] == null) continue;
                                if (parsedStormReplay.HeroesAttr.ContainsKey(ban[j].ToString()))
                                {
                                    ban[j] = parsedStormReplay.HeroesAttr[ban[j].ToString()];

                                }

                            }
                        }

                        if (parsedStormReplay.OverallData.Bans != null)
                        {
                            for (var i = 0; i < parsedStormReplay.OverallData.Bans.Length; i++)
                            {
                                for (var j = 0; j < parsedStormReplay.OverallData.Bans[i].Length; j++)
                                {
                                    using var cmd = conn.CreateCommand();
                                    var value = "0";

                                    if (parsedStormReplay.OverallData.Bans[i][j] != null)
                                    {
                                        value = parsedStormReplay.Heroes[parsedStormReplay.OverallData.Bans[i][j].ToString()];
                                    }

                                    cmd.CommandText = "INSERT INTO replay_bans (replayID, team, hero) VALUES(" +
                                                      parsedStormReplay.ReplayId + "," +
                                                      i + "," +
                                                      value + ")";
                                    cmd.CommandTimeout = 0;
                                    //Console.WriteLine(cmd.CommandText);
                                    var reader = cmd.ExecuteReader();
                                }
                            }
                        }


                        for (var i = 0; i < parsedStormReplay.OverallData.TeamExperience.Length; i++)
                        {
                            for (var j = 0; j < parsedStormReplay.OverallData.TeamExperience[i].Length; j++)
                            {
                                using var cmd = conn.CreateCommand();
                                cmd.CommandText = "INSERT into replay_experience_breakdown (replayID, team, team_level, timestamp, structureXP, creepXP, heroXP, minionXP, trickXP, totalXP) VALUES(" +
                                                  "\"" + parsedStormReplay.ReplayId + "\"" + "," +
                                                  "\"" + i + "\"" + "," +
                                                  "\"" + parsedStormReplay.OverallData.TeamExperience[i][j].TeamLevel + "\"" + "," +
                                                  "\"" + parsedStormReplay.OverallData.TeamExperience[i][j].TimeSpan.ToString("HH:mm:ss") + "\"" + "," +
                                                  "\"" + parsedStormReplay.OverallData.TeamExperience[i][j].StructureXp + "\"" + "," +
                                                  "\"" + parsedStormReplay.OverallData.TeamExperience[i][j].CreepXp + "\"" + "," +
                                                  "\"" + parsedStormReplay.OverallData.TeamExperience[i][j].HeroXp + "\"" + "," +
                                                  "\"" + parsedStormReplay.OverallData.TeamExperience[i][j].MinionXp + "\"" + "," +
                                                  "\"" + parsedStormReplay.OverallData.TeamExperience[i][j].TrickleXp + "\"" + "," +
                                                  "\"" + parsedStormReplay.OverallData.TeamExperience[i][j].TotalXp + "\"" + ")";
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

                    for (var i = 0; i < parsedStormReplay.OverallData.Players.Length; i++)
                    {
                        foreach (var item in parsedStormReplay.OverallData.Players[i].HeroLevelTaunt)
                        {
                            if (!parsedStormReplay.HeroesAttr.ContainsKey(item.HeroAttributeId)) continue;
                            if (parsedStormReplay.HeroesAttr[item.HeroAttributeId] != parsedStormReplay.OverallData.Players[i].Hero) continue;
                            parsedStormReplay.OverallData.Players[i].MasteyTauntTier = item.TierLevel;
                            break;
                        }


                        for (var j = 0; j < parsedStormReplay.OverallData.Players.Length; j++)
                        {
                            if (j == i) continue;
                            if (parsedStormReplay.OverallData.Players[i].Hero != parsedStormReplay.OverallData.Players[j].Hero) continue;
                            parsedStormReplay.OverallData.Players[i].Mirror = 1;
                            break;
                        }
                    }

                    var playerTable = isBrawl ? "heroesprofile_brawl.player" : "player";
                    var scoresTable = isBrawl ? "heroesprofile_brawl.scores" : "scores";
                    var talentsTable = isBrawl ? "heroesprofile_brawl.talents" : "talents";

                    foreach (var player in parsedStormReplay.OverallData.Players)
                    {
                        player.WinnerValue = player.Winner ? "1" : "0";
                        //Console.WriteLine("Saving Player Information for:" + data.Players.Battletag);
                        using (var cmd = conn.CreateCommand())
                        {
                            cmd.CommandText = "INSERT INTO " + playerTable + " (" +
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
                                              parsedStormReplay.ReplayId + "," +
                                              player.BlizzId + "," +
                                              player.battletag_table_id + "," +
                                              parsedStormReplay.Heroes[player.Hero] + "," +
                                              player.HeroLevel + "," +
                                              player.MasteyTauntTier + "," +
                                              player.Team + "," +
                                              player.WinnerValue + "," +
                                              player.Party +
                                              ")";
                            cmd.CommandTimeout = 0;
                            var reader = cmd.ExecuteReader();
                        }

                        //Console.WriteLine("Saving Score Information for:" + data.Players.Battletag);
                        if (player.Score != null)
                        {
                            using var cmd = conn.CreateCommand();
                            cmd.CommandText = "INSERT INTO " + scoresTable + " (" +
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
                                              parsedStormReplay.ReplayId + "," +
                                              player.battletag_table_id + "," +
                                              CheckIfEmpty(player.Score.Level) + "," +
                                              CheckIfEmpty(player.Score.SoloKills) + "," +
                                              CheckIfEmpty(player.Score.Assists) + "," +
                                              CheckIfEmpty(player.Score.Takedowns) + "," +
                                              CheckIfEmpty(player.Score.Deaths) + "," +
                                              CheckIfEmpty(player.Score.HighestKillStreak) + "," +
                                              CheckIfEmpty(player.Score.HeroDamage) + "," +
                                              CheckIfEmpty(player.Score.SiegeDamage) + "," +
                                              CheckIfEmpty(player.Score.StructureDamage) + "," +
                                              CheckIfEmpty(player.Score.MinionDamage) + "," +
                                              CheckIfEmpty(player.Score.CreepDamage) + "," +
                                              CheckIfEmpty(player.Score.SummonDamage) + "," +
                                              CheckIfEmpty(Convert.ToInt64(player.Score.TimeCCdEnemyHeroes_not_null.UtcDateTime.TimeOfDay.TotalSeconds)) + "," +
                                              CheckIfEmpty(player.Score.Healing) + "," +
                                              CheckIfEmpty(player.Score.SelfHealing) + "," +
                                              CheckIfEmpty(player.Score.DamageTaken) + "," +
                                              CheckIfEmpty(player.Score.ExperienceContribution) + "," +
                                              CheckIfEmpty(player.Score.TownKills) + "," +
                                              CheckIfEmpty(Convert.ToInt64(player.Score.TimeSpentDead.UtcDateTime.TimeOfDay.TotalSeconds)) + "," +

                                              CheckIfEmpty(player.Score.MercCampCaptures) + "," +
                                              CheckIfEmpty(player.Score.WatchTowerCaptures) + "," +
                                              CheckIfEmpty(player.Score.MetaExperience) + ",";
                            if (player.Score.MatchAwards.Length > 0)
                            {
                                cmd.CommandText += player.Score.MatchAwards[0] + ",";

                            }
                            else
                            {
                                cmd.CommandText += "NULL" + ",";
                            }

                            cmd.CommandText += CheckIfEmpty(player.Score.ProtectionGivenToAllies) + "," +
                                               CheckIfEmpty(player.Score.TimeSilencingEnemyHeroes) + "," +
                                               CheckIfEmpty(player.Score.TimeRootingEnemyHeroes) + "," +
                                               CheckIfEmpty(player.Score.TimeStunningEnemyHeroes) + "," +
                                               CheckIfEmpty(player.Score.ClutchHealsPerformed) + "," +
                                               CheckIfEmpty(player.Score.EscapesPerformed) + "," +
                                               CheckIfEmpty(player.Score.VengeancesPerformed) + "," +
                                               CheckIfEmpty(player.Score.OutnumberedDeaths) + "," +
                                               CheckIfEmpty(player.Score.TeamfightEscapesPerformed) + "," +
                                               CheckIfEmpty(player.Score.TeamfightHealingDone) + "," +
                                               CheckIfEmpty(player.Score.TeamfightDamageTaken) + "," +
                                               CheckIfEmpty(player.Score.TeamfightHeroDamage) + "," +

                                               CheckIfEmpty(player.Score.Multikill) + "," +
                                               CheckIfEmpty(player.Score.PhysicalDamage) + "," +
                                               CheckIfEmpty(player.Score.SpellDamage) + "," +
                                               CheckIfEmpty(player.Score.RegenGlobes) + "," +
                                               CheckIfEmpty(player.Score.FirstToTen) + ")";




                            cmd.CommandTimeout = 0;
                            //Console.WriteLine(cmd.CommandText);
                            var reader = cmd.ExecuteReader();
                        }

                        //Console.WriteLine("Saving Talent Information for:" + data.Players.Battletag);
                        using (var cmd = conn.CreateCommand())
                        {
                            if (player.Talents != null)
                            {
                                cmd.CommandText = player.Talents.Length switch
                                {
                                        0 => ("INSERT INTO " + talentsTable + " (replayID, battletag, level_one, level_four, level_seven, level_ten, level_thirteen, level_sixteen, level_twenty) VALUES(" +
                                              parsedStormReplay.ReplayId + "," + player.battletag_table_id + "," + "NULL" + "," + "NULL" + "," + "NULL" + "," + "NULL" + "," + "NULL" + "," + "NULL" +
                                              "," + "NULL" + ")"),
                                        1 => ("INSERT INTO " + talentsTable + " (replayID, battletag, level_one) VALUES(" + parsedStormReplay.ReplayId + "," + player.battletag_table_id + "," +
                                              parsedStormReplay.Talents[player.Hero + "|" + player.Talents[0]] + ")"),
                                        2 => ("INSERT INTO " + talentsTable + " (replayID, battletag, level_one, level_four) VALUES(" + parsedStormReplay.ReplayId + "," + player.battletag_table_id + "," +
                                              parsedStormReplay.Talents[player.Hero + "|" + player.Talents[0]] + "," + parsedStormReplay.Talents[player.Hero + "|" + player.Talents[1]] + ")"),
                                        3 => ("INSERT INTO " + talentsTable + " (replayID, battletag, level_one, level_four, level_seven) VALUES(" + parsedStormReplay.ReplayId + "," + player.battletag_table_id +
                                              "," + parsedStormReplay.Talents[player.Hero + "|" + player.Talents[0]] + "," + parsedStormReplay.Talents[player.Hero + "|" + player.Talents[1]] + "," +
                                              parsedStormReplay.Talents[player.Hero + "|" + player.Talents[2]] + ")"),
                                        4 => ("INSERT INTO " + talentsTable + " (replayID, battletag, level_one, level_four, level_seven, level_ten) VALUES(" + parsedStormReplay.ReplayId + "," +
                                              player.battletag_table_id + "," + parsedStormReplay.Talents[player.Hero + "|" + player.Talents[0]] + "," +
                                              parsedStormReplay.Talents[player.Hero + "|" + player.Talents[1]] + "," + parsedStormReplay.Talents[player.Hero + "|" + player.Talents[2]] + "," +
                                              parsedStormReplay.Talents[player.Hero + "|" + player.Talents[3]] + ")"),
                                        5 => ("INSERT INTO " + talentsTable + " (replayID, battletag, level_one, level_four, level_seven, level_ten, level_thirteen) VALUES(" + parsedStormReplay.ReplayId + "," +
                                              player.battletag_table_id + "," + parsedStormReplay.Talents[player.Hero + "|" + player.Talents[0]] + "," +
                                              parsedStormReplay.Talents[player.Hero + "|" + player.Talents[1]] + "," + parsedStormReplay.Talents[player.Hero + "|" + player.Talents[2]] + "," +
                                              parsedStormReplay.Talents[player.Hero + "|" + player.Talents[3]] + "," + parsedStormReplay.Talents[player.Hero + "|" + player.Talents[4]] + ")"),
                                        6 => ("INSERT INTO " + talentsTable + " (replayID, battletag, level_one, level_four, level_seven, level_ten, level_thirteen, level_sixteen) VALUES(" +
                                              parsedStormReplay.ReplayId + "," + player.battletag_table_id + "," + parsedStormReplay.Talents[player.Hero + "|" + player.Talents[0]] + "," +
                                              parsedStormReplay.Talents[player.Hero + "|" + player.Talents[1]] + "," + parsedStormReplay.Talents[player.Hero + "|" + player.Talents[2]] + "," +
                                              parsedStormReplay.Talents[player.Hero + "|" + player.Talents[3]] + "," + parsedStormReplay.Talents[player.Hero + "|" + player.Talents[4]] + "," +
                                              parsedStormReplay.Talents[player.Hero + "|" + player.Talents[5]] + ")"),
                                        7 => ("INSERT INTO " + talentsTable + " (replayID, battletag, level_one, level_four, level_seven, level_ten, level_thirteen, level_sixteen, level_twenty) VALUES(" +
                                              parsedStormReplay.ReplayId + "," + player.battletag_table_id + "," + parsedStormReplay.Talents[player.Hero + "|" + player.Talents[0]] + "," +
                                              parsedStormReplay.Talents[player.Hero + "|" + player.Talents[1]] + "," + parsedStormReplay.Talents[player.Hero + "|" + player.Talents[2]] + "," +
                                              parsedStormReplay.Talents[player.Hero + "|" + player.Talents[3]] + "," + parsedStormReplay.Talents[player.Hero + "|" + player.Talents[4]] + "," +
                                              parsedStormReplay.Talents[player.Hero + "|" + player.Talents[5]] + "," + parsedStormReplay.Talents[player.Hero + "|" + player.Talents[6]] + ")"),
                                        _ => cmd.CommandText
                                };

                                cmd.CommandTimeout = 0;
                                //Console.WriteLine(cmd.CommandText);
                                var reader = cmd.ExecuteReader();
                            }
                            else
                            {
                                cmd.CommandText = "INSERT INTO " + talentsTable + " (replayID, battletag, level_one, level_four, level_seven, level_ten, level_thirteen, level_sixteen, level_twenty) VALUES(" +
                                                  parsedStormReplay.ReplayId + "," +
                                                  player.battletag_table_id + "," +
                                                  "NULL" + "," +
                                                  "NULL" + "," +
                                                  "NULL" + "," +
                                                  "NULL" + "," +
                                                  "NULL" + "," +
                                                  "NULL" + "," +
                                                  "NULL" + ")";

                                cmd.CommandTimeout = 0;
                                //Console.WriteLine(cmd.CommandText);
                                var reader = cmd.ExecuteReader();
                            }

                        }
                    }
                    //saveMasterMMRData(data, conn);

                    if (parsedStormReplay.SeasonsGameVersions.ContainsKey(parsedStormReplay.OverallData.Version))
                    {
                        if (Convert.ToInt32(parsedStormReplay.SeasonsGameVersions[parsedStormReplay.OverallData.Version]) < 13) return;
                        if (isBrawl)
                        {
                            UpdateGlobalHeroData(parsedStormReplay.OverallData, conn);
                            UpdateGlobalTalentData(parsedStormReplay, conn);
                            UpdateGlobalTalentDataDetails(parsedStormReplay, conn);
                        }
                        else
                        {
                            UpdateGameModeTotalGames(Convert.ToInt32(parsedStormReplay.SeasonsGameVersions[parsedStormReplay.OverallData.Version]), parsedStormReplay, conn);
                            InsertUrlIntoReplayUrls(parsedStormReplay, conn);
                        }
                        
                    }
                    else
                    {
                        var season = SaveToSeasonGameVersion(DateTime.Parse(parsedStormReplay.OverallData.Date.ToString("yyyy-MM-dd HH:mm:ss")), parsedStormReplay.OverallData.Version, parsedStormReplay, conn);
                        parsedStormReplay.SeasonsGameVersions.Add(parsedStormReplay.OverallData.Version, season);
                        //Save Game Version to table
                        //Add it to dic
                        if (Convert.ToInt32(parsedStormReplay.SeasonsGameVersions[parsedStormReplay.OverallData.Version]) < 13) return;
                        if (isBrawl)
                        {
                            UpdateGlobalHeroData(parsedStormReplay.OverallData, conn);
                            UpdateGlobalTalentData(parsedStormReplay, conn);
                            UpdateGlobalTalentDataDetails(parsedStormReplay, conn);
                        }
                        else
                        {
                            UpdateGameModeTotalGames(Convert.ToInt32(parsedStormReplay.SeasonsGameVersions[parsedStormReplay.OverallData.Version]), parsedStormReplay, conn);
                            InsertUrlIntoReplayUrls(parsedStormReplay, conn);
                        }

                    }

                }
                else
                {
                    if (badHeroName)
                    {
                        Console.WriteLine("Bad Hero Name - Saving in Replays Not Processed");
                        InsertIntoReplaysNotProcessed(parsedStormReplay.ReplayId.ToString(), "NULL", parsedStormReplay.OverallData.Region.ToString(), 
                                parsedStormReplay.OverallData.Mode, parsedStormReplay.OverallData.Length.UtcDateTime.TimeOfDay.TotalSeconds.ToString(),
                                parsedStormReplay.OverallData.Date.ToString("yyyy-MM-dd HH:mm:ss"), parsedStormReplay.OverallData.Map,
                                parsedStormReplay.OverallData.Version, "0", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), parsedStormReplay.ReplayUrl.ToString(),
                                "NULL", "Bad Hero Name");

                        //save as bad hero name
                    }
                    else if (badTalentName)
                    {
                        Console.WriteLine("Bad Talent Name - Saving in Replays Not Processed");
                        InsertIntoReplaysNotProcessed(parsedStormReplay.ReplayId.ToString(), "NULL", parsedStormReplay.OverallData.Region.ToString(),
                                parsedStormReplay.OverallData.Mode, parsedStormReplay.OverallData.Length.UtcDateTime.TimeOfDay.TotalSeconds.ToString(),
                                parsedStormReplay.OverallData.Date.ToString("yyyy-MM-dd HH:mm:ss"), parsedStormReplay.OverallData.Map,
                                parsedStormReplay.OverallData.Version, "0", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), parsedStormReplay.ReplayUrl.ToString(),
                                "NULL", "Bad Talent Name");

                        //save as bad talent name
                    }
                    else
                    {
                        Console.WriteLine("Unknown Failure - Saving in Replays Not Processed");
                        //Save in replays not processed
                        InsertIntoReplaysNotProcessed(parsedStormReplay.ReplayId.ToString(), "NULL", parsedStormReplay.OverallData.Region.ToString(), 
                                parsedStormReplay.OverallData.Mode, parsedStormReplay.OverallData.Length.UtcDateTime.TimeOfDay.TotalSeconds.ToString(),
                                parsedStormReplay.OverallData.Date.ToString("yyyy-MM-dd HH:mm:ss"), parsedStormReplay.OverallData.Map,
                                parsedStormReplay.OverallData.Version, "0",
                                DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), parsedStormReplay.ReplayUrl.ToString(),
                                "NULL", "Undetermined");
                    }

                }

            }
            catch (Exception e)
            {
                using var conn = new MySqlConnection(_connectionString);
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText =
                        "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES(" +
                        parsedStormReplay.ReplayId + "," +
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
                        "\"" + parsedStormReplay.ReplayUrl + "\"" + "," +
                        "\"" + "NULL" + "\"" + "," +
                        "\"" + e + "\"" + ")";
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
                var reader = cmd.ExecuteReader();
            }

        }

        private static string CheckIfEmpty(long? value)
        {
            return value == null ? "NULL" : value.ToString();
        }

        private static void UpdateGlobalHeroData(LambdaJson.ReplayData data, MySqlConnection conn)
        {
            //

            foreach (var player in data.Players)
            {
                var winLoss = player.Winner ? 1 : 0;


                if (player.Score == null) continue;
                var heroLevel = 0;

                if (player.HeroLevel < 5)
                {
                    heroLevel = 1;
                    heroLevel = 1;

                }
                else if (player.HeroLevel >= 5 && player.HeroLevel < 10)
                {
                    heroLevel = 5;
                }
                else if (player.HeroLevel >= 10 && player.HeroLevel < 15)
                {
                    heroLevel = 10;
                }
                else if (player.HeroLevel >= 15 && player.HeroLevel < 20)
                {
                    heroLevel = 15;
                }
                else if (player.HeroLevel >= 20)
                {
                    heroLevel = 20;
                }

                using var cmd = conn.CreateCommand();
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
                                  "region, " +
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
                                  "\"" + data.Version + "\"" + ",";
                if (data.Mode != "Brawl")
                {
                    cmd.CommandText += "\"" + data.GameType_id + "\"" + "," +
                                       "\"" + player.player_league_tier + "\"" + ",";
                }
                else
                {
                    cmd.CommandText += "\"" + "-1" + "\"" + "," +
                                       "\"" + "0" + "\"" + "," +
                                       "\"" + "0" + "\"" + "," +
                                       "\"" + "0" + "\"" + ",";
                }

                cmd.CommandText += "\"" + data.GameMap_id + "\"" + "," +
                                   "\"" + heroLevel + "\"" + "," +
                                   "\"" + player.Hero_id + "\"" + "," +
                                   "\"" + player.Mirror + "\"" + "," +
                                   "\"" + data.Region + "\"" + "," +
                                   "\"" + winLoss + "\"" + "," +

                                   "\"" + data.Length.UtcDateTime.TimeOfDay.TotalSeconds + "\"" + "," +
                                   CheckIfEmpty(player.Score.SoloKills) + "," +
                                   CheckIfEmpty(player.Score.Assists) + "," +
                                   CheckIfEmpty(player.Score.Takedowns) + "," +
                                   CheckIfEmpty(player.Score.Deaths) + "," +
                                   CheckIfEmpty(player.Score.HighestKillStreak) + "," +
                                   CheckIfEmpty(player.Score.HeroDamage) + "," +
                                   CheckIfEmpty(player.Score.SiegeDamage) + "," +
                                   CheckIfEmpty(player.Score.StructureDamage) + "," +
                                   CheckIfEmpty(player.Score.MinionDamage) + "," +
                                   CheckIfEmpty(player.Score.CreepDamage) + "," +
                                   CheckIfEmpty(player.Score.SummonDamage) + "," +
                                   CheckIfEmpty(Convert.ToInt64(player.Score.TimeCCdEnemyHeroes_not_null.UtcDateTime.TimeOfDay.TotalSeconds)) + "," +
                                   CheckIfEmpty(player.Score.Healing) + "," +
                                   CheckIfEmpty(player.Score.SelfHealing) + "," +
                                   CheckIfEmpty(player.Score.DamageTaken) + "," +
                                   CheckIfEmpty(player.Score.ExperienceContribution) + "," +
                                   CheckIfEmpty(player.Score.TownKills) + "," +
                                   CheckIfEmpty(Convert.ToInt64(player.Score.TimeSpentDead.UtcDateTime.TimeOfDay.TotalSeconds)) + "," +
                                   CheckIfEmpty(player.Score.MercCampCaptures) + "," +
                                   CheckIfEmpty(player.Score.WatchTowerCaptures) + "," +
                                   CheckIfEmpty(player.Score.ProtectionGivenToAllies) + "," +
                                   CheckIfEmpty(player.Score.TimeSilencingEnemyHeroes) + "," +
                                   CheckIfEmpty(player.Score.TimeRootingEnemyHeroes) + "," +
                                   CheckIfEmpty(player.Score.TimeStunningEnemyHeroes) + "," +
                                   CheckIfEmpty(player.Score.ClutchHealsPerformed) + "," +
                                   CheckIfEmpty(player.Score.EscapesPerformed) + "," +
                                   CheckIfEmpty(player.Score.VengeancesPerformed) + "," +
                                   CheckIfEmpty(player.Score.OutnumberedDeaths) + "," +
                                   CheckIfEmpty(player.Score.TeamfightEscapesPerformed) + "," +
                                   CheckIfEmpty(player.Score.TeamfightHealingDone) + "," +
                                   CheckIfEmpty(player.Score.TeamfightDamageTaken) + "," +
                                   CheckIfEmpty(player.Score.TeamfightHeroDamage) + "," +

                                   CheckIfEmpty(player.Score.Multikill) + "," +
                                   CheckIfEmpty(player.Score.PhysicalDamage) + "," +
                                   CheckIfEmpty(player.Score.SpellDamage) + "," +
                                   CheckIfEmpty(player.Score.RegenGlobes) + "," +
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
                var reader = cmd.ExecuteReader();
            }
        }
        private int InsertTalentCombo(string hero, int levelOne, int levelFour, int levelSeven, int levelTen, int levelThirteen, int levelSixteen, int levelTwenty)
        {
            var combId = 0;

            using (var conn = new MySqlConnection(_connectionString))
            {
                conn.Open();
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "INSERT INTO talent_combination_id (hero, level_one, level_four, level_seven, level_ten, level_thirteen, level_sixteen, level_twenty) VALUES (" +
                                      hero + "," +
                                      levelOne + "," +
                                      levelFour + "," +
                                      levelSeven + "," +
                                      levelTen + "," +
                                      levelThirteen + "," +
                                      levelSixteen + "," +
                                      levelTwenty +
                                      ")";

                    var reader = cmd.ExecuteReader();
                }


                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT talent_combination_id FROM heroesprofile.talent_combinations WHERE " +
                                      "hero = " + hero +
                                      " AND level_one = " + levelOne +
                                      " AND level_four = " + levelFour +
                                      " AND level_seven = " + levelSeven +
                                      " AND level_ten = " + levelTen +
                                      " AND level_thirteen = " + levelThirteen +
                                      " AND level_sixteen = " + levelSixteen +
                                      " AND level_twenty = " + levelTwenty;

                    var reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        combId = reader.GetInt32("talent_combination_id");
                    }
                }
            }

            return combId;
        }

        private int GetHeroCombId(string hero, int levelOne, int levelFour, int levelSeven, int levelTen, int levelThirteen, int levelSixteen, int levelTwenty)
        {
            var combId = 0;
            using (var conn = new MySqlConnection(_connectionString))
            {
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT talent_combination_id FROM heroesprofile.talent_combinations WHERE " +
                                  "hero = " + hero +
                                  " AND level_one = " + levelOne +
                                  " AND level_four = " + levelFour +
                                  " AND level_seven = " + levelSeven +
                                  " AND level_ten = " + levelTen +
                                  " AND level_thirteen = " + levelThirteen +
                                  " AND level_sixteen = " + levelSixteen +
                                  " AND level_twenty = " + levelTwenty;

                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    combId = reader.GetInt32("talent_combination_id");
                }

                if (!reader.HasRows)
                {
                    combId = InsertTalentCombo(hero, levelOne, levelFour, levelSeven, levelTen, levelThirteen, levelSixteen, levelTwenty);
                }
            }

            return combId;
        }

        private void UpdateGlobalTalentData(ParsedStormReplay parsedStormReplay, MySqlConnection conn)
        {
            foreach (var player in parsedStormReplay.OverallData.Players)
            {
                var winLoss = 0;
                winLoss = player.Winner ? 1 : 0;

                if (player.Score == null) continue;
                var heroLevel = 0;

                if (player.HeroLevel < 5)
                {
                    heroLevel = 1;
                }
                else if (player.HeroLevel >= 5 && player.HeroLevel < 10)
                {
                    heroLevel = 5;
                }
                else if (player.HeroLevel >= 10 && player.HeroLevel < 15)
                {
                    heroLevel = 10;
                }
                else if (player.HeroLevel >= 15 && player.HeroLevel < 20)
                {
                    heroLevel = 15;
                }
                else if (player.HeroLevel >= 20)
                {
                    heroLevel = player.MasteyTauntTier switch
                    {
                            0 => 20,
                            1 => 25,
                            2 => 40,
                            3 => 60,
                            4 => 80,
                            5 => 100,
                            _ => heroLevel
                    };
                }

                using var cmd = conn.CreateCommand();
                cmd.CommandText = "INSERT INTO global_hero_talents (" +
                                  "game_version, " +
                                  "game_type, " +
                                  "league_tier, " +
                                  "hero_league_tier, " +
                                  "role_league_tier, " +
                                  "game_map, " +
                                  "hero_level, " +
                                  "hero, " +
                                  "mirror, " +
                                  "region, " +
                                  "win_loss, " +
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
                                  "\"" + parsedStormReplay.OverallData.Version + "\"" + ",";
                cmd.CommandText += "\"" + parsedStormReplay.OverallData.GameType_id + "\"" + "," +
                                   "\"" + 0 + "\"" + "," +
                                   "\"" + 0 + "\"" + "," +
                                   "\"" + 0 + "\"" + "," +
                                   "\"" + parsedStormReplay.OverallData.GameMap_id + "\"" + "," +
                                   "\"" + heroLevel + "\"" + "," +
                                   "\"" + player.Hero_id + "\"" + "," +
                                   "\"" + player.Mirror + "\"" + "," +
                                   "\"" + parsedStormReplay.OverallData.Region + "\"" + "," +
                                   "\"" + winLoss + "\"" + ",";



                if (player.Talents?[0] == null || player.Talents[0] == "")
                {
                    cmd.CommandText += GetHeroCombId(
                                               player.Hero_id,
                                               0,
                                               0,
                                               0,
                                               0,
                                               0,
                                               0,
                                               0) + ",";
                }
                else if (player.Talents[1] == null || player.Talents[1] == "")
                {
                    cmd.CommandText += GetHeroCombId(
                                               player.Hero_id,
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[0]]),
                                               0,
                                               0,
                                               0,
                                               0,
                                               0,
                                               0) + ",";

                }
                else if (player.Talents[2] == null || player.Talents[2] == "")
                {
                    cmd.CommandText += GetHeroCombId(
                                               player.Hero_id,
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[0]]),
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[1]]),
                                               0,
                                               0,
                                               0,
                                               0,
                                               0) + ",";
                }
                else if (player.Talents[3] == null || player.Talents[3] == "")
                {
                    cmd.CommandText += GetHeroCombId(
                                               player.Hero_id,
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[0]]),
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[1]]),
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[2]]),
                                               0,
                                               0,
                                               0,
                                               0) + ",";
                }
                else if (player.Talents[4] == null || player.Talents[4] == "")
                {
                    cmd.CommandText += GetHeroCombId(
                                               player.Hero_id,
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[0]]),
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[1]]),
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[2]]),
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[3]]),
                                               0,
                                               0,
                                               0) + ",";

                }
                else if (player.Talents[5] == null || player.Talents[5] == "")
                {
                    cmd.CommandText += GetHeroCombId(
                                               player.Hero_id,
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[0]]),
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[1]]),
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[2]]),
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[3]]),
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[4]]),
                                               0,
                                               0) + ",";
                }
                else if (player.Talents[6] == null || player.Talents[6] == "")
                {
                    cmd.CommandText += GetHeroCombId(
                                               player.Hero_id,
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[0]]),
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[1]]),
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[2]]),
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[3]]),
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[4]]),
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[5]]),
                                               0) + ",";
                }
                else
                {
                    cmd.CommandText += GetHeroCombId(
                                               player.Hero_id,
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[0]]),
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[1]]),
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[2]]),
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[3]]),
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[4]]),
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[5]]),
                                               Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[6]])) + ",";
                }

                cmd.CommandText += "\"" + parsedStormReplay.OverallData.Length.UtcDateTime.TimeOfDay.TotalSeconds + "\"" + "," +
                                   CheckIfEmpty(player.Score.SoloKills) + "," +
                                   CheckIfEmpty(player.Score.Assists) + "," +
                                   CheckIfEmpty(player.Score.Takedowns) + "," +
                                   CheckIfEmpty(player.Score.Deaths) + "," +
                                   CheckIfEmpty(player.Score.HighestKillStreak) + "," +
                                   CheckIfEmpty(player.Score.HeroDamage) + "," +
                                   CheckIfEmpty(player.Score.SiegeDamage) + "," +
                                   CheckIfEmpty(player.Score.StructureDamage) + "," +
                                   CheckIfEmpty(player.Score.MinionDamage) + "," +
                                   CheckIfEmpty(player.Score.CreepDamage) + "," +
                                   CheckIfEmpty(player.Score.SummonDamage) + "," +
                                   CheckIfEmpty(Convert.ToInt64(player.Score.TimeCCdEnemyHeroes_not_null.UtcDateTime.TimeOfDay.TotalSeconds)) + "," +
                                   CheckIfEmpty(player.Score.Healing) + "," +
                                   CheckIfEmpty(player.Score.SelfHealing) + "," +
                                   CheckIfEmpty(player.Score.DamageTaken) + "," +
                                   CheckIfEmpty(player.Score.ExperienceContribution) + "," +
                                   CheckIfEmpty(player.Score.TownKills) + "," +
                                   CheckIfEmpty(Convert.ToInt64(player.Score.TimeSpentDead.UtcDateTime.TimeOfDay.TotalSeconds)) + "," +
                                   CheckIfEmpty(player.Score.MercCampCaptures) + "," +
                                   CheckIfEmpty(player.Score.WatchTowerCaptures) + "," +
                                   CheckIfEmpty(player.Score.ProtectionGivenToAllies) + "," +
                                   CheckIfEmpty(player.Score.TimeSilencingEnemyHeroes) + "," +
                                   CheckIfEmpty(player.Score.TimeRootingEnemyHeroes) + "," +
                                   CheckIfEmpty(player.Score.TimeStunningEnemyHeroes) + "," +
                                   CheckIfEmpty(player.Score.ClutchHealsPerformed) + "," +
                                   CheckIfEmpty(player.Score.EscapesPerformed) + "," +
                                   CheckIfEmpty(player.Score.VengeancesPerformed) + "," +
                                   CheckIfEmpty(player.Score.OutnumberedDeaths) + "," +
                                   CheckIfEmpty(player.Score.TeamfightEscapesPerformed) + "," +
                                   CheckIfEmpty(player.Score.TeamfightHealingDone) + "," +
                                   CheckIfEmpty(player.Score.TeamfightDamageTaken) + "," +
                                   CheckIfEmpty(player.Score.TeamfightHeroDamage) + "," +

                                   CheckIfEmpty(player.Score.Multikill) + "," +
                                   CheckIfEmpty(player.Score.PhysicalDamage) + "," +
                                   CheckIfEmpty(player.Score.SpellDamage) + "," +
                                   CheckIfEmpty(player.Score.RegenGlobes) + "," +
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
                var reader = cmd.ExecuteReader();
            }
        }

        private static void UpdateGameModeTotalGames(int season, ParsedStormReplay parsedStormReplay, MySqlConnection conn)
        {
            foreach (var player in parsedStormReplay.OverallData.Players)
            {
                var wins = 0;
                var losses = 0;
                if (player.Winner)
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
                                      "\"" + parsedStormReplay.MmrIds["player"] + "\"" + "," +
                                      season + "," +
                                      parsedStormReplay.OverallData.GameType_id + "," +
                                      player.BlizzId + "," +
                                      parsedStormReplay.OverallData.Region + "," +
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
                    var reader = cmd.ExecuteReader();
                }


                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "INSERT INTO master_games_played_data (type_value, season, game_type, blizz_id, region, win, loss, games_played) VALUES (" +
                                      "\"" + parsedStormReplay.MmrIds[parsedStormReplay.Role[player.Hero]] + "\"" + "," +
                                      season + "," +
                                      parsedStormReplay.OverallData.GameType_id + "," +
                                      player.BlizzId + "," +
                                      parsedStormReplay.OverallData.Region + "," +
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
                    var reader = cmd.ExecuteReader();
                }

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "INSERT INTO master_games_played_data (type_value, season, game_type, blizz_id, region, win, loss, games_played) VALUES (" +
                                      "\"" + parsedStormReplay.MmrIds[player.Hero] + "\"" + "," +
                                      season + "," +
                                      parsedStormReplay.OverallData.GameType_id + "," +
                                      player.BlizzId + "," +
                                      parsedStormReplay.OverallData.Region + "," +
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
                    var reader = cmd.ExecuteReader();
                }
            }
        }

        private static void UpdateGlobalTalentDataDetails(ParsedStormReplay parsedStormReplay, MySqlConnection conn)
        {
            for (var i = 0; i < parsedStormReplay.OverallData.Players.Length; i++)
            {
                for (var j = 0; j < parsedStormReplay.OverallData.Players.Length; j++)
                {
                    if (j == i) continue;
                    if (parsedStormReplay.OverallData.Players[i].Hero != parsedStormReplay.OverallData.Players[j].Hero) continue;
                    parsedStormReplay.OverallData.Players[i].Mirror = 1;
                    break;
                }
            }

            foreach (var player in parsedStormReplay.OverallData.Players)
            {
                var winLoss = player.Winner ? 1 : 0;

                if (player.Talents == null) continue;
                for (var t = 0; t < 7; t++)
                {
                    var level = t switch
                    {
                            0 => "1",
                            1 => "4",
                            2 => "7",
                            3 => "10",
                            4 => "13",
                            5 => "16",
                            6 => "20",
                            _ => ""
                    };

                    if (player.Score == null) continue;
                    var heroLevel = 0;

                    if (player.HeroLevel < 5)
                    {
                        heroLevel = 1;
                    }
                    else if (player.HeroLevel >= 5 && player.HeroLevel < 10)
                    {
                        heroLevel = 5;
                    }
                    else if (player.HeroLevel >= 10 && player.HeroLevel < 15)
                    {
                        heroLevel = 10;
                    }
                    else if (player.HeroLevel >= 15 && player.HeroLevel < 20)
                    {
                        heroLevel = 15;
                    }
                    else if (player.HeroLevel >= 20)
                    {
                        heroLevel = player.MasteyTauntTier switch
                        {
                                0 => 20,
                                1 => 25,
                                2 => 40,
                                3 => 60,
                                4 => 80,
                                5 => 100,
                                _ => heroLevel
                        };
                    }

                    using var cmd = conn.CreateCommand();
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
                                      "region, " +
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
                                      "\"" + parsedStormReplay.OverallData.Version + "\"" + "," +
                                      parsedStormReplay.OverallData.GameType_id + "," +
                                      0 + "," +
                                      0 + "," +
                                      0 + "," +
                                      parsedStormReplay.OverallData.GameMap_id + "," +
                                      heroLevel + "," +
                                      player.Hero_id + "," +
                                      player.Mirror + "," +
                                      parsedStormReplay.OverallData.Region + "," +
                                      "\"" + winLoss + "\"" + "," +
                                      level + ",";

                    switch (t)
                    {
                        case 0:
                            cmd.CommandText += Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[0]]) + ",";
                            break;
                        case 1:
                            cmd.CommandText += Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[1]]) + ",";
                            break;
                        case 2:
                            cmd.CommandText += Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[2]]) + ",";
                            break;
                        case 3:
                            cmd.CommandText += Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[3]]) + ",";
                            break;
                        case 4:
                            cmd.CommandText += Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[4]]) + ",";
                            break;
                        case 5:
                            cmd.CommandText += Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[5]]) + ",";
                            break;
                        case 6:
                            cmd.CommandText += Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[6]]) + ",";
                            break;
                    }


                    cmd.CommandText += "\"" + parsedStormReplay.OverallData.Length.UtcDateTime.TimeOfDay.TotalSeconds + "\"" + "," +
                                       CheckIfEmpty(player.Score.SoloKills) + "," +
                                       CheckIfEmpty(player.Score.Assists) + "," +
                                       CheckIfEmpty(player.Score.Takedowns) + "," +
                                       CheckIfEmpty(player.Score.Deaths) + "," +
                                       CheckIfEmpty(player.Score.HighestKillStreak) + "," +
                                       CheckIfEmpty(player.Score.HeroDamage) + "," +
                                       CheckIfEmpty(player.Score.SiegeDamage) + "," +
                                       CheckIfEmpty(player.Score.StructureDamage) + "," +
                                       CheckIfEmpty(player.Score.MinionDamage) + "," +
                                       CheckIfEmpty(player.Score.CreepDamage) + "," +
                                       CheckIfEmpty(player.Score.SummonDamage) + "," +
                                       CheckIfEmpty(Convert.ToInt64(player.Score.TimeCCdEnemyHeroes_not_null.UtcDateTime.TimeOfDay.TotalSeconds)) + "," +
                                       CheckIfEmpty(player.Score.Healing) + "," +
                                       CheckIfEmpty(player.Score.SelfHealing) + "," +
                                       CheckIfEmpty(player.Score.DamageTaken) + "," +
                                       CheckIfEmpty(player.Score.ExperienceContribution) + "," +
                                       CheckIfEmpty(player.Score.TownKills) + "," +
                                       CheckIfEmpty(Convert.ToInt64(player.Score.TimeSpentDead.UtcDateTime.TimeOfDay.TotalSeconds)) + "," +
                                       CheckIfEmpty(player.Score.MercCampCaptures) + "," +
                                       CheckIfEmpty(player.Score.WatchTowerCaptures) + "," +
                                       CheckIfEmpty(player.Score.ProtectionGivenToAllies) + "," +
                                       CheckIfEmpty(player.Score.TimeSilencingEnemyHeroes) + "," +
                                       CheckIfEmpty(player.Score.TimeRootingEnemyHeroes) + "," +
                                       CheckIfEmpty(player.Score.TimeStunningEnemyHeroes) + "," +
                                       CheckIfEmpty(player.Score.ClutchHealsPerformed) + "," +
                                       CheckIfEmpty(player.Score.EscapesPerformed) + "," +
                                       CheckIfEmpty(player.Score.VengeancesPerformed) + "," +
                                       CheckIfEmpty(player.Score.OutnumberedDeaths) + "," +
                                       CheckIfEmpty(player.Score.TeamfightEscapesPerformed) + "," +
                                       CheckIfEmpty(player.Score.TeamfightHealingDone) + "," +
                                       CheckIfEmpty(player.Score.TeamfightDamageTaken) + "," +
                                       CheckIfEmpty(player.Score.TeamfightHeroDamage) + "," +

                                       CheckIfEmpty(player.Score.Multikill) + "," +
                                       CheckIfEmpty(player.Score.PhysicalDamage) + "," +
                                       CheckIfEmpty(player.Score.SpellDamage) + "," +
                                       CheckIfEmpty(player.Score.RegenGlobes) + "," +
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
                    cmd.ExecuteReader();
                }
            }
        }

        private static string InsertIntoTalentTable(string hero, string talentName, Dictionary<string, string> heroesAlt, MySqlConnection conn)
        {
            if (hero == "")
            {
                var split = Regex.Split(talentName, @"(?<!^)(?=[A-Z])");
                hero = heroesAlt.ContainsKey(split[0]) ? heroesAlt[split[0]] : split[0];
            }

            var id = "";
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                        "INSERT IGNORE INTO heroes_data_talents (hero_name, short_name, attribute_id, title, talent_name, description, status, hotkey, cooldown, mana_cost, sort, level, icon) VALUES " +
                        "(" +
                        "\"" + hero + "\"" + "," +
                        "\"" + "" + "\"" + "," +
                        "\"" + "" + "\"" + "," +
                        "\"" + "" + "\"" + "," +
                        "\"" + talentName + "\"" + "," +
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

                cmd.ExecuteReader();
            }


            using (var cmd = conn.CreateCommand())
            {

                cmd.CommandText = "SELECT talent_id from heroes_data_talents where hero_name = " + "\"" + hero + "\"" + " AND talent_name = " + "\"" + talentName + "\"" + "  order by talent_id asc";
                cmd.CommandTimeout = 0;
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    id = reader.GetString("talent_id");
                }
            }

            return id;
        }

        private static void InsertUrlIntoReplayUrls(ParsedStormReplay parsedStormReplay, MySqlConnection conn)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "INSERT INTO replay_urls (replayID, game_date, game_type, url) VALUES (" +
                              "\"" + parsedStormReplay.ReplayId + "\"" + "," +
                              "\"" + parsedStormReplay.OverallData.Date.ToString("yyyy-MM-dd HH:mm:ss") + "\"" + "," +
                              "\"" + parsedStormReplay.OverallData.GameType_id + "\"" + "," +
                              "\"" + parsedStormReplay.ReplayUrl + "\"" + ")";
            cmd.ExecuteReader();
        }

        private string SaveToSeasonGameVersion(DateTime gameDate, string gameVersion, ParsedStormReplay parsedStormReplay, MySqlConnection conn)
        {
            var season = "";

            foreach (var s in parsedStormReplay.Seasons.Keys.Where(s => gameDate >= parsedStormReplay.Seasons[s][0] && gameDate < parsedStormReplay.Seasons[s][1]))
            {
                season = s;
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "INSERT IGNORE INTO season_game_versions (season, game_version, date_added) VALUES (" +
                    season + "," +
                    "\"" + gameVersion + "\"" + "," +
                    "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" +
                    ")";
                Console.WriteLine(cmd.CommandText);
                cmd.ExecuteReader();
            }
            return season;
        }

        private void InsertIntoReplaysNotProcessed(string replayId, string parsedId, string region, string gameType, string gameLength, string gameDate, string gameMap, string gameVersion,
                                                   string size, string parsedDate, string url, string processed, string errorMessage)
        {
            using var conn = new MySqlConnection(_connectionString);
            conn.Open();

            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                    "INSERT INTO replays_not_processed (replayId, parsedID, region, game_type, game_length, game_date, game_map, game_version, size, date_parsed, count_parsed, url, processed, failure_status) VALUES (" +
                    "\"" + replayId + "\"" + "," +
                    "\"" + parsedId + "\"" + "," +
                    "\"" + region + "\"" + "," +
                    "\"" + gameType + "\"" + "," +
                    "\"" + gameLength + "\"" + "," +
                    "\"" + gameDate + "\"" + "," +
                    "\"" + gameMap + "\"" + "," +
                    "\"" + gameVersion + "\"" + "," +
                    "\"" + size + "\"" + "," +
                    "\"" + parsedDate + "\"" + "," +
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
            var reader = cmd.ExecuteReader();
        }

        public void ParseStormReplay(Uri replayUrl)
        {
            var globalJson = "";
            var httpWebRequest = (HttpWebRequest) WebRequest.Create(_apiSettings.lambda_parser_endpoint_url);

            httpWebRequest.Method = "POST";
            httpWebRequest.Timeout = 1000000;

            using (var streamWriter = new StreamWriter(httpWebRequest.GetRequestStream()))
            {
                var json = JsonConvert.SerializeObject(new
                {
                        //input = "http://hotsapi.s3-website-eu-west-1.amazonaws.com/c5a49c21-d3d0-c8d9-c904-b3d09feea5e9.StormReplay",
                        input = replayUrl,
                        access = _apiSettings.lambda_parser_endpoint_access,
                        secret = _apiSettings.lambda_parser_endpoint_secret 
                });

                streamWriter.Write(json);
            }

            var httpResponse = (HttpWebResponse) httpWebRequest.GetResponse();
            using (var streamReader = new StreamReader(httpResponse.GetResponseStream()))
            {
                var result = streamReader.ReadToEnd();
                Console.WriteLine(result);
                globalJson = result;
            }

            var data = LambdaJson.ReplayData.FromJson(globalJson);
        }
    }
}