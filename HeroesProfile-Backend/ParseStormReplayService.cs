using System;
using System.Collections.Generic;
using System.Net;
using System.IO;
using System.Linq;
using MySql.Data.MySqlClient;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using HeroesProfile_Backend.Models;
using HeroesProfileDb.HeroesProfile;
using HeroesProfileDb.HeroesProfileBrawl;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;
using Z.EntityFramework.Plus;
using Replay = HeroesProfileDb.HeroesProfile.Replay;
using ReplaysNotProcessed = HeroesProfile_Backend.Models.ReplaysNotProcessed;

namespace HeroesProfile_Backend
{
    public class ParseStormReplayService
    {
        private readonly ApiSettings _apiSettings;
        private readonly HeroesProfileContext _context;
        private readonly HeroesProfileBrawlContext _brawlContext;

        public ParseStormReplayService(ApiSettings apiSettings, HeroesProfileContext context, HeroesProfileBrawlContext brawlContext)
        {
            _apiSettings = apiSettings;
            _context = context;
            _brawlContext = brawlContext;
        }

        public async Task<ParsedStormReplay> ParseStormReplay(long replayId, Uri replayUrl, HotsApiJSON.ReplayData hotsapiData, Dictionary<string, string> maps, 
                                                  Dictionary<string, string> mapsTranslations, Dictionary<string, string> gameTypes, Dictionary<string, string> talents, 
                                                  Dictionary<string, string> seasonsGameVersions, Dictionary<string, string> mmrIds, Dictionary<string, DateTime[]> seasons,
                                                  Dictionary<string, string> heroes, Dictionary<string, string> heroesTranslations, Dictionary<string, string> mapsShort,
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

                await using (var streamWriter = new StreamWriter(httpWebRequest.GetRequestStream()))
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

                if (Regex.Match(result, "Error parsing replay: UnexpectedResult").Success ||
                    Regex.Match(result, "Error parsing replay: SuccessReplayDetail").Success ||
                    Regex.Match(result, "Error parsing replay: ParserException").Success)
                {
                    await InsertNotProcessedReplay((int)replayId, null, (int?)hotsapiData.Region, hotsapiData.GameType,
                        hotsapiData.GameLength.ToString(), Convert.ToDateTime(hotsapiData.GameDate), hotsapiData.GameMap,
                        hotsapiData.GameVersion, hotsapiData.Size.ToString(), DateTime.Now, 1, replayUrl.ToString(),
                        hotsapiData.Processed.ToString(), result);
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
                                parsedReplay.Dupe = data.Mode == "Brawl" ? await _context.Replay.AnyAsync(x => x.ReplayId == replayId)
                                    : await _brawlContext.Replay.AnyAsync(x => x.ReplayId == replayId);
                               
                            }
                            else
                            {
                                await UpsertNotProcessedReplay((int) replayId, null, (int?) hotsapiData.Region,
                                    hotsapiData.GameType,
                                    hotsapiData.GameLength.ToString(), Convert.ToDateTime(hotsapiData.GameDate),
                                    hotsapiData.GameMap,
                                    hotsapiData.GameVersion, hotsapiData.Size.ToString(), DateTime.Now, 1,
                                    replayUrl.ToString(),
                                    hotsapiData.Processed.ToString(), "Map or Game Type Bad");
                            }

                        }
                        else
                        {
                            await UpsertNotProcessedReplay((int)replayId, null, (int?)hotsapiData.Region,
                                hotsapiData.GameType,
                                hotsapiData.GameLength.ToString(), Convert.ToDateTime(hotsapiData.GameDate),
                                hotsapiData.GameMap,
                                hotsapiData.GameVersion, hotsapiData.Size.ToString(), DateTime.Now, 1,
                                replayUrl.ToString(),
                                hotsapiData.Processed.ToString(), "Map or Game Type Bad");
                        }
                    }
                    else
                    {
                        await UpsertNotProcessedReplay((int)replayId, null, (int?)hotsapiData.Region,
                                hotsapiData.GameType,
                                hotsapiData.GameLength.ToString(), Convert.ToDateTime(hotsapiData.GameDate),
                                hotsapiData.GameMap,
                                "", hotsapiData.Size.ToString(), DateTime.Now, 1,
                                replayUrl.ToString(),
                                hotsapiData.Processed.ToString(), "Game Version Null");
                    }
                }
            }
            catch (Exception e)
            {
                await InsertNotProcessedReplay((int)replayId, null, 0, null,
                        null, Convert.ToDateTime(hotsapiData.GameDate), null,
                        null, null, DateTime.Now, 1, replayUrl.ToString(),
                        null, e.ToString());
            }

            return parsedReplay;
        }

        public async Task<ParsedStormReplay> ParseStormReplay(ReplaysNotProcessed replay, Dictionary<string, string> maps, Dictionary<string, string> mapsTranslations,
                                Dictionary<string, string> gameTypes, Dictionary<string, string> talents, Dictionary<string, string> seasonsGameVersions, Dictionary<string, string> mmrIds,
                                Dictionary<string, DateTime[]> seasons, Dictionary<string, string> heroes, Dictionary<string, string> heroesTranslations, Dictionary<string, string> mapsShort,
                                Dictionary<string, string> mmrs, Dictionary<string, string> roles, Dictionary<string, string> heroesAttr)
        {
            var replayUrl = new Uri(replay.url, UriKind.Absolute);
            var replayId = Convert.ToInt64(replay.replayID);

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

                await using (var streamWriter = new StreamWriter(httpWebRequest.GetRequestStream()))
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


                if (Regex.Match(result, "Error parsing replay: UnexpectedResult").Success
                || Regex.Match(result, "Error parsing replay: SuccessReplayDetail").Success
                    || Regex.Match(result, "Error parsing replay: ParserException").Success)
                {
                    await UpsertNotProcessedReplayIncrementCountParsed((int) replayId, null, 0, null, null,
                            DateTime.Now, null, null, null, DateTime.Now, 1, replayUrl.ToString(), null,
                            result);
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

                            player.Score.TimeCCdEnemyHeroes_not_null =
                                    DateTimeOffset.TryParse(player.Score.TimeCCdEnemyHeroes, out var dateValue)
                                            ? dateValue
                                            : DateTimeOffset.Parse("00:00:00");
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
                                bool dupe;

                                if (data.Mode != "Brawl")
                                {
                                    dupe = await _context.Replay.AnyAsync(x => x.ReplayId == replayId);
                                }
                                else
                                {
                                    dupe = await _brawlContext.Replay.AnyAsync(x => x.ReplayId == replayId);
                                }

                                parsedReplay.Dupe = dupe;
                            }
                            else
                            {
                                await UpsertNotProcessedReplayIncrementCountParsed((int) replayId, null, 0, null, null,
                                        DateTime.Now, null, null, null, DateTime.Now, 1, replayUrl.ToString(),
                                        null, "Map or Game Type Bad");
                            }

                        }
                        else
                        {
                            await UpsertNotProcessedReplayIncrementCountParsed((int) replayId, null, 0, null, null,
                                    DateTime.Now, null, null, null, DateTime.Now, 1, replayUrl.ToString(),
                                    null, "Map or Game Type Bad");
                        }
                    }
                    else
                    {
                        await UpsertNotProcessedReplayIncrementCountParsed((int) replayId, null, 0, null, null,
                                DateTime.Now, null, null, null, DateTime.Now, 1, replayUrl.ToString(),
                                null, "Game Version Null");
                    }
                }

                if (!parsedReplay.Dupe) return parsedReplay;
                {
                    await _context.ReplaysNotProcessed.Where(x => x.ReplayId == replayId).DeleteAsync();
                }
            }
            catch (Exception e)
            {
                await UpsertNotProcessedReplayIncrementCountParsed((int)replayId, null, 0, null, null,
                        DateTime.Now, null, null, null, DateTime.Now, 1, replayUrl.ToString(),
                        null, e.ToString());
            }

            return parsedReplay;
        }

        public async Task SaveReplayData(ParsedStormReplay parsedStormReplay, bool isBrawl)
        {
            try
            {
                if (isBrawl)
                {
                    parsedStormReplay.Data.GameType_id = "-1";
                }
                if (parsedStormReplay.OverallData.Players == null) return;

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
                            parsedStormReplay.Talents.Add(player.Hero + "|" + talent, await InsertIntoTalentTable(player.Hero, talent, parsedStormReplay.HeroesAlt));
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
                        var battletag = new Battletags
                        {
                                BlizzId = (int) player.BlizzId,
                                Battletag = player.BattletagName + "#" + player.BattletagId,
                                Region = (sbyte) parsedStormReplay.OverallData.Region,
                                AccountLevel = (int?) player.AccountLevel,
                                LatestGame = Convert.ToDateTime(parsedStormReplay.OverallData.Date)
                        };
                        await _context.Battletags.Upsert(battletag)
                                      .WhenMatched(x => new Battletags
                                      {
                                              AccountLevel = x.AccountLevel < battletag.AccountLevel
                                                      ? battletag.AccountLevel
                                                      : x.AccountLevel,
                                              LatestGame = x.LatestGame < battletag.LatestGame
                                                      ? battletag.LatestGame
                                                      : x.LatestGame
                                      }).RunAsync();

                        player.battletag_table_id =
                                (await _context.Battletags.FirstOrDefaultAsync(x =>
                                        x.Battletag == player.BattletagName + "#" +
                                        player.BattletagId)).Battletag;
                    }


                    if (isBrawl)
                    {
                        await _brawlContext.Replay.AddAsync(new HeroesProfileDb.HeroesProfileBrawl.Replay
                        {
                                ReplayId = (int) parsedStormReplay.ReplayId,
                                GameDate = Convert.ToDateTime(parsedStormReplay.OverallData.Date),
                                GameLength = (short) parsedStormReplay
                                                     .OverallData.Length.UtcDateTime.TimeOfDay.TotalSeconds,
                                GameMap = Convert.ToSByte(parsedStormReplay.Maps[parsedStormReplay.OverallData.Map]),
                                GameVersion = parsedStormReplay.OverallData.Version,
                                Region = (sbyte) parsedStormReplay.OverallData.Region,
                                DateAdded = DateTime.Now
                        });
                        await _brawlContext.SaveChangesAsync();
                    }
                    else
                    {
                        await _context.Replay.AddAsync(new Replay
                        {
                                ReplayId = (uint) parsedStormReplay.ReplayId,
                                GameDate = Convert.ToDateTime(parsedStormReplay.OverallData.Date),
                                ParsedId = null,
                                GameType = Convert.ToByte(
                                        parsedStormReplay.GameTypes[parsedStormReplay.OverallData.Mode]),
                                GameLength = (ushort) parsedStormReplay
                                                      .OverallData.Length.UtcDateTime.TimeOfDay.TotalSeconds,
                                GameMap = Convert.ToByte(parsedStormReplay.Maps[parsedStormReplay.OverallData.Map]),
                                GameVersion = parsedStormReplay.OverallData.Version,
                                Region = (byte) parsedStormReplay.OverallData.Region,
                                DateAdded = DateTime.Now
                        });
                        await _context.SaveChangesAsync();
                    }

                    await _context.ReplaysNotProcessed.Where(x => x.ReplayId == parsedStormReplay.ReplayId).DeleteAsync();

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
                                    var value = "0";

                                    if (parsedStormReplay.OverallData.Bans[i][j] != null)
                                    {
                                        value = parsedStormReplay.Heroes[
                                                parsedStormReplay.OverallData.Bans[i][j].ToString()];
                                    }

                                    await _context.ReplayBans.AddAsync(new ReplayBans
                                    {
                                            ReplayId = (int) parsedStormReplay.ReplayId,
                                            Team = (byte) i,
                                            Hero = Convert.ToUInt32(value)
                                    });
                                }
                            }

                            await _context.SaveChangesAsync();
                        }


                        for (var i = 0; i < parsedStormReplay.OverallData.TeamExperience.Length; i++)
                        {
                            for (var j = 0; j < parsedStormReplay.OverallData.TeamExperience[i].Length; j++)
                            {
                                var breakdown = new ReplayExperienceBreakdown
                                {
                                        ReplayId = (int) parsedStormReplay.ReplayId,
                                        Team = (sbyte) i,
                                        TeamLevel = (int) parsedStormReplay.OverallData.TeamExperience[i][j].TeamLevel,
                                        Timestamp = parsedStormReplay.OverallData.TeamExperience[i][j].TimeSpan
                                                                     .ToString("HH:mm:ss"),
                                        StructureXp = parsedStormReplay.OverallData.TeamExperience[i][j].StructureXp,
                                        CreepXp = parsedStormReplay.OverallData.TeamExperience[i][j].CreepXp,
                                        HeroXp = parsedStormReplay.OverallData.TeamExperience[i][j].HeroXp,
                                        MinionXp = parsedStormReplay.OverallData.TeamExperience[i][j].MinionXp,
                                        TrickXp = parsedStormReplay.OverallData.TeamExperience[i][j].TrickleXp,
                                        TotalXp = parsedStormReplay.OverallData.TeamExperience[i][j].TotalXp
                                };
                                await _context.ReplayExperienceBreakdown.Upsert(breakdown)
                                              .WhenMatched(x => new ReplayExperienceBreakdown
                                              {
                                                      ReplayId = breakdown.ReplayId,
                                                      Team = breakdown.Team,
                                                      TeamLevel = breakdown.TeamLevel,
                                                      Timestamp = breakdown.Timestamp,
                                                      StructureXp = breakdown.StructureXp,
                                                      CreepXp = breakdown.CreepXp,
                                                      HeroXp = breakdown.HeroXp,
                                                      MinionXp = breakdown.MinionXp,
                                                      TrickXp = breakdown.TrickXp,
                                                      TotalXp = breakdown.TotalXp
                                              }).RunAsync();
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

                    foreach (var player in parsedStormReplay.OverallData.Players)
                    {
                        player.WinnerValue = player.Winner ? "1" : "0";

                        if (isBrawl)
                        {
                            await _brawlContext.Player.AddAsync(new HeroesProfileDb.HeroesProfileBrawl.Player
                            {
                                    ReplayId = (int) parsedStormReplay.ReplayId,
                                    BlizzId = (int) player.BlizzId,
                                    Battletag = player.battletag_table_id,
                                    Hero = Convert.ToSByte(parsedStormReplay.Heroes[player.Hero]),
                                    HeroLevel = (short) player.HeroLevel,
                                    MasteryTaunt = (short) player.MasteyTauntTier,
                                    Team = (sbyte) player.Team,
                                    Winner = Convert.ToSByte(player.WinnerValue),
                                    Party = player.Party.ToString()
                            });
                        }
                        else
                        {
                            await _context.Player.AddAsync(new HeroesProfileDb.HeroesProfile.Player
                            {
                                    ReplayId = (uint) parsedStormReplay.ReplayId,
                                    BlizzId = (uint) player.BlizzId,
                                    Battletag = player.battletag_table_id,
                                    Hero = Convert.ToByte(parsedStormReplay.Heroes[player.Hero]),
                                    HeroLevel = (ushort) player.HeroLevel,
                                    MasteryTaunt = (ushort?) player.MasteyTauntTier,
                                    Team = (byte) player.Team,
                                    Winner = Convert.ToByte(player.WinnerValue),
                                    Party = player.Party.ToString()
                            });
                        }

                        if (player.Score != null)
                        {
                            if (isBrawl)
                            {
                                await _brawlContext.Scores.AddAsync(new HeroesProfileDb.HeroesProfileBrawl.Scores
                                {
                                        ReplayId = (int) parsedStormReplay.ReplayId,
                                        Battletag = player.battletag_table_id,
                                        Level = (int) player.Score.Level,
                                        Kills = (int) player.Score.SoloKills,
                                        Assists = (int) player.Score.Assists,
                                        Takedowns = (int) player.Score.Takedowns,
                                        Deaths = (int) player.Score.Deaths,
                                        HighestKillStreak = (int) player.Score.HighestKillStreak,
                                        HeroDamage = (int) player.Score.HeroDamage,
                                        SiegeDamage = (int) player.Score.SiegeDamage,
                                        StructureDamage = (int) player.Score.StructureDamage,
                                        MinionDamage = (int) player.Score.MinionDamage,
                                        CreepDamage = (int) player.Score.CreepDamage,
                                        SummonDamage = (int) player.Score.SummonDamage,
                                        TimeCcEnemyHeroes = (int) player
                                                                  .Score.TimeCCdEnemyHeroes_not_null.UtcDateTime
                                                                  .TimeOfDay.TotalSeconds,
                                        Healing = (int) player.Score.Healing,
                                        SelfHealing = (int) player.Score.SelfHealing,
                                        DamageTaken = (int) player.Score.DamageTaken,
                                        ExperienceContribution = (int) player.Score.ExperienceContribution,
                                        TownKills = (int) player.Score.TownKills,
                                        TimeSpentDead =
                                                (int) player.Score.TimeSpentDead.UtcDateTime.TimeOfDay.TotalSeconds,
                                        MercCampCaptures = (int) player.Score.MercCampCaptures,
                                        WatchTowerCaptures = (int) player.Score.WatchTowerCaptures,
                                        MetaExperience = (int) player.Score.MetaExperience,
                                        MatchAward = (int) (player.Score.MatchAwards.Length > 0
                                                ? player.Score.MatchAwards[0]
                                                : 0),
                                        ProtectionAllies = (int) player.Score.ProtectionGivenToAllies,
                                        SilencingEnemies = (int) player.Score.TimeSilencingEnemyHeroes,
                                        RootingEnemies = (int) player.Score.TimeRootingEnemyHeroes,
                                        StunningEnemies = (int) player.Score.TimeStunningEnemyHeroes,
                                        ClutchHeals = (int) player.Score.ClutchHealsPerformed,
                                        Escapes = (int) player.Score.EscapesPerformed,
                                        Vengeance = (int) player.Score.VengeancesPerformed,
                                        OutnumberedDeaths = (int) player.Score.OutnumberedDeaths,
                                        TeamfightEscapes = (int) player.Score.TeamfightEscapesPerformed,
                                        TeamfightHealing = (int) player.Score.TeamfightHealingDone,
                                        TeamfightDamageTaken = (int) player.Score.TeamfightDamageTaken,
                                        TeamfightHeroDamage = (int) player.Score.TeamfightHeroDamage,
                                        Multikill = (int) player.Score.Multikill,
                                        PhysicalDamage = (int) player.Score.PhysicalDamage,
                                        SpellDamage = (int) player.Score.SpellDamage,
                                        RegenGlobes = (int) player.Score.RegenGlobes,
                                        FirstToTen = (int) player.Score.FirstToTen
                                });
                            }
                            else
                            {
                                await _context.Scores.AddAsync(new HeroesProfileDb.HeroesProfile.Scores
                                {
                                        ReplayId = (uint) parsedStormReplay.ReplayId,
                                        Battletag = player.battletag_table_id,
                                        Level = (int) player.Score.Level,
                                        Kills = (int) player.Score.SoloKills,
                                        Assists = (int) player.Score.Assists,
                                        Takedowns = (int) player.Score.Takedowns,
                                        Deaths = (int) player.Score.Deaths,
                                        HighestKillStreak = (int) player.Score.HighestKillStreak,
                                        HeroDamage = (int) player.Score.HeroDamage,
                                        SiegeDamage = (int) player.Score.SiegeDamage,
                                        StructureDamage = (int) player.Score.StructureDamage,
                                        MinionDamage = (int) player.Score.MinionDamage,
                                        CreepDamage = (int) player.Score.CreepDamage,
                                        SummonDamage = (int) player.Score.SummonDamage,
                                        TimeCcEnemyHeroes = (int) player
                                                                  .Score.TimeCCdEnemyHeroes_not_null.UtcDateTime
                                                                  .TimeOfDay.TotalSeconds,
                                        Healing = (int) player.Score.Healing,
                                        SelfHealing = (int) player.Score.SelfHealing,
                                        DamageTaken = (int) player.Score.DamageTaken,
                                        ExperienceContribution = (int) player.Score.ExperienceContribution,
                                        TownKills = (int) player.Score.TownKills,
                                        TimeSpentDead =
                                                (int) player.Score.TimeSpentDead.UtcDateTime.TimeOfDay.TotalSeconds,
                                        MercCampCaptures = (int) player.Score.MercCampCaptures,
                                        WatchTowerCaptures = (int) player.Score.WatchTowerCaptures,
                                        MetaExperience = (int) player.Score.MetaExperience,
                                        MatchAward =
                                                (player.Score.MatchAwards.Length > 0 ? player.Score.MatchAwards[0] : 0)
                                                .ToString(),
                                        ProtectionAllies = (int) player.Score.ProtectionGivenToAllies,
                                        SilencingEnemies = (int) player.Score.TimeSilencingEnemyHeroes,
                                        RootingEnemies = (int) player.Score.TimeRootingEnemyHeroes,
                                        StunningEnemies = (int) player.Score.TimeStunningEnemyHeroes,
                                        ClutchHeals = (int) player.Score.ClutchHealsPerformed,
                                        Escapes = (int) player.Score.EscapesPerformed,
                                        Vengeance = (int) player.Score.VengeancesPerformed,
                                        OutnumberedDeaths = (int) player.Score.OutnumberedDeaths,
                                        TeamfightEscapes = (int) player.Score.TeamfightEscapesPerformed,
                                        TeamfightHealing = (int) player.Score.TeamfightHealingDone,
                                        TeamfightDamageTaken = (int) player.Score.TeamfightDamageTaken,
                                        TeamfightHeroDamage = (int) player.Score.TeamfightHeroDamage,
                                        Multikill = (int) player.Score.Multikill,
                                        PhysicalDamage = (int) player.Score.PhysicalDamage,
                                        SpellDamage = (int) player.Score.SpellDamage,
                                        RegenGlobes = (int) player.Score.RegenGlobes,
                                        FirstToTen = (sbyte?) player.Score.FirstToTen
                                });
                            }
                        }

                        if (isBrawl)
                        {
                            await _brawlContext.Talents.AddAsync(new HeroesProfileDb.HeroesProfileBrawl.Talents
                            {
                                    ReplayId = (int) parsedStormReplay.ReplayId,
                                    Battletag = player.battletag_table_id,
                                    LevelOne = player.Talents != null && player.Talents.Length > 0
                                            ? Convert.ToInt32(
                                                    parsedStormReplay.Talents
                                                            [player.Hero + "|" + player.Talents[0]])
                                            : 0,
                                    LevelFour = player.Talents != null && player.Talents.Length > 1
                                            ? Convert.ToInt32(
                                                    parsedStormReplay.Talents
                                                            [player.Hero + "|" + player.Talents[1]])
                                            : 0,
                                    LevelSeven = player.Talents != null && player.Talents.Length > 2
                                            ? Convert.ToInt32(
                                                    parsedStormReplay.Talents
                                                            [player.Hero + "|" + player.Talents[2]])
                                            : 0,
                                    LevelTen = player.Talents != null && player.Talents.Length > 3
                                            ? Convert.ToInt32(
                                                    parsedStormReplay.Talents
                                                            [player.Hero + "|" + player.Talents[3]])
                                            : 0,
                                    LevelThirteen = player.Talents != null && player.Talents.Length > 4
                                            ? Convert.ToInt32(
                                                    parsedStormReplay.Talents
                                                            [player.Hero + "|" + player.Talents[4]])
                                            : 0,
                                    LevelSixteen = player.Talents != null && player.Talents.Length > 5
                                            ? Convert.ToInt32(
                                                    parsedStormReplay.Talents
                                                            [player.Hero + "|" + player.Talents[5]])
                                            : 0,
                                    LevelTwenty = player.Talents != null && player.Talents.Length > 6
                                            ? Convert.ToInt32(
                                                    parsedStormReplay.Talents
                                                            [player.Hero + "|" + player.Talents[6]])
                                            : 0,
                            });
                        }
                        else
                        {
                            await _context.Talents.AddAsync(new HeroesProfileDb.HeroesProfile.Talents
                            {
                                    ReplayId = (uint) parsedStormReplay.ReplayId,
                                    Battletag = Convert.ToInt32(player.battletag_table_id),
                                    LevelOne = player.Talents != null && player.Talents.Length > 0
                                            ? Convert.ToInt32(
                                                    parsedStormReplay.Talents
                                                            [player.Hero + "|" + player.Talents[0]])
                                            : 0,
                                    LevelFour = player.Talents != null && player.Talents.Length > 1
                                            ? Convert.ToInt32(
                                                    parsedStormReplay.Talents
                                                            [player.Hero + "|" + player.Talents[1]])
                                            : 0,
                                    LevelSeven = player.Talents != null && player.Talents.Length > 2
                                            ? Convert.ToInt32(
                                                    parsedStormReplay.Talents
                                                            [player.Hero + "|" + player.Talents[2]])
                                            : 0,
                                    LevelTen = player.Talents != null && player.Talents.Length > 3
                                            ? Convert.ToInt32(
                                                    parsedStormReplay.Talents
                                                            [player.Hero + "|" + player.Talents[3]])
                                            : 0,
                                    LevelThirteen = player.Talents != null && player.Talents.Length > 4
                                            ? Convert.ToInt32(
                                                    parsedStormReplay.Talents
                                                            [player.Hero + "|" + player.Talents[4]])
                                            : 0,
                                    LevelSixteen = player.Talents != null && player.Talents.Length > 5
                                            ? Convert.ToInt32(
                                                    parsedStormReplay.Talents
                                                            [player.Hero + "|" + player.Talents[5]])
                                            : 0,
                                    LevelTwenty = player.Talents != null && player.Talents.Length > 6
                                            ? Convert.ToInt32(
                                                    parsedStormReplay.Talents
                                                            [player.Hero + "|" + player.Talents[6]])
                                            : 0
                            });
                        }
                        //Save all the stuff we inserted above
                        await _context.SaveChangesAsync();
                        await _brawlContext.SaveChangesAsync();

                    }
                    //saveMasterMMRData(data, conn);

                    if (parsedStormReplay.SeasonsGameVersions.ContainsKey(parsedStormReplay.OverallData.Version))
                    {
                        if (Convert.ToInt32(parsedStormReplay.SeasonsGameVersions[parsedStormReplay.OverallData.Version]) < 13) return;
                        if (isBrawl)
                        {
                            await UpdateGlobalHeroData(parsedStormReplay.OverallData);
                            await UpdateGlobalTalentData(parsedStormReplay);
                            await UpdateGlobalTalentDataDetails(parsedStormReplay);
                        }
                        else
                        {
                            await UpdateGameModeTotalGames(Convert.ToInt32(parsedStormReplay.SeasonsGameVersions[parsedStormReplay.OverallData.Version]), parsedStormReplay);
                            await InsertUrlIntoReplayUrls(parsedStormReplay);
                        }
                        
                    }
                    else
                    {
                        var season = await SaveToSeasonGameVersion(Convert.ToDateTime(parsedStormReplay.OverallData.Date), parsedStormReplay.OverallData.Version, parsedStormReplay);
                        parsedStormReplay.SeasonsGameVersions.Add(parsedStormReplay.OverallData.Version, season);
                        //Save Game Version to table
                        //Add it to dic
                        if (Convert.ToInt32(parsedStormReplay.SeasonsGameVersions[parsedStormReplay.OverallData.Version]) < 13) return;
                        if (isBrawl)
                        {
                            await UpdateGlobalHeroData(parsedStormReplay.OverallData);
                            await UpdateGlobalTalentData(parsedStormReplay);
                            await UpdateGlobalTalentDataDetails(parsedStormReplay);
                        }
                        else
                        {
                            await UpdateGameModeTotalGames(Convert.ToInt32(parsedStormReplay.SeasonsGameVersions[parsedStormReplay.OverallData.Version]), parsedStormReplay);
                            await InsertUrlIntoReplayUrls(parsedStormReplay);
                        }

                    }

                }
                else
                {
                    string failureStatus;
                    if (badHeroName)
                    {
                        failureStatus = "Bad Hero Name";
                    }
                    else if (badTalentName)
                    {
                        failureStatus = "Bad Talent Name";
                    }
                    else
                    {
                        failureStatus = "Undetermined";
                    }

                    Console.WriteLine(failureStatus + " failure - Saving in Replays Not Processed");
                    await UpsertNotProcessedReplay((int) parsedStormReplay.ReplayId, "NULL",
                            (int?) parsedStormReplay.OverallData.Region, parsedStormReplay.OverallData.Mode,
                            parsedStormReplay.OverallData.Length.UtcDateTime.TimeOfDay.TotalSeconds.ToString(),
                            Convert.ToDateTime(parsedStormReplay.OverallData.Date),
                            parsedStormReplay.OverallData.Map, parsedStormReplay.OverallData.Version, "0",
                            DateTime.Now, 1, parsedStormReplay.ReplayUrl.ToString(),
                            "NULL", failureStatus);
                }

            }
            catch (Exception e)
            {
                await UpsertNotProcessedReplay((int) parsedStormReplay.ReplayId, null, 0, null, null, DateTime.Now,
                        null, null, "0", DateTime.Now, 1, parsedStormReplay.ReplayUrl.ToString(), null, e.ToString());
            }
        }

        private async Task UpdateGlobalHeroData(LambdaJson.ReplayData data)
        {
            foreach (var player in data.Players)
            {
                var winLoss = player.Winner ? 1 : 0;

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
                    heroLevel = 20;
                }

                var globalHeroStats = new GlobalHeroStats
                {
                    GameVersion = data.Version,
                    GameType = data.Mode != "Brawl" ? Convert.ToSByte(data.GameType_id) : Convert.ToSByte (-1),
                    LeagueTier = data.Mode != "Brawl" ? Convert.ToSByte(player.player_league_tier) : Convert.ToSByte(0),
                    HeroLeagueTier = data.Mode != "Brawl" ? Convert.ToSByte(player.hero_league_tier) : Convert.ToSByte(0),
                    RoleLeagueTier = data.Mode != "Brawl" ? Convert.ToSByte(player.role_league_tier) : Convert.ToSByte(0),
                    GameMap = Convert.ToSByte(data.GameMap_id),
                    HeroLevel = (uint)heroLevel,
                    Hero = Convert.ToSByte(player.Hero_id),
                    Mirror = (sbyte)player.Mirror,
                    Region = Convert.ToSByte(data.Region),
                    WinLoss = (sbyte)winLoss,
                    GameTime = (uint?)data.Length.UtcDateTime.TimeOfDay.TotalSeconds,
                    Kills = (uint?)player.Score.SoloKills,
                    Assists = (uint?)player.Score.Assists,
                    Takedowns = (uint?)player.Score.Takedowns,
                    Deaths = (uint?)player.Score.Deaths,
                    HighestKillStreak = (uint?)player.Score.HighestKillStreak,
                    HeroDamage = (uint?)player.Score.HeroDamage,
                    SiegeDamage = (uint?)player.Score.SiegeDamage,
                    StructureDamage = (uint?)player.Score.StructureDamage,
                    MinionDamage = (uint?)player.Score.MinionDamage,
                    CreepDamage = (uint?)player.Score.CreepDamage,
                    SummonDamage = (uint?)player.Score.SummonDamage,
                    TimeCcEnemyHeroes = (uint?)player.Score.TimeCCdEnemyHeroes_not_null.UtcDateTime.TimeOfDay.TotalSeconds,
                    Healing = (uint?)player.Score.Healing,
                    SelfHealing = (uint?)player.Score.SelfHealing,
                    DamageTaken = (uint?)player.Score.DamageTaken,
                    ExperienceContribution = (uint?)player.Score.ExperienceContribution,
                    TownKills = (uint?)player.Score.TownKills,
                    TimeSpentDead = (uint?)player.Score.TimeSpentDead.UtcDateTime.TimeOfDay.TotalSeconds,
                    MercCampCaptures = (uint?)player.Score.MercCampCaptures,
                    WatchTowerCaptures = (uint?)player.Score.WatchTowerCaptures,
                    ProtectionAllies = (uint?)player.Score.ProtectionGivenToAllies,
                    SilencingEnemies = (uint?)player.Score.TimeSilencingEnemyHeroes,
                    RootingEnemies = (uint?)player.Score.TimeRootingEnemyHeroes,
                    StunningEnemies = (uint?)player.Score.TimeStunningEnemyHeroes,
                    ClutchHeals = (uint?)player.Score.ClutchHealsPerformed,
                    Escapes = (uint?)player.Score.EscapesPerformed,
                    Vengeance = (uint?)player.Score.VengeancesPerformed,
                    OutnumberedDeaths = (uint?)player.Score.OutnumberedDeaths,
                    TeamfightEscapes = (uint?)player.Score.TeamfightEscapesPerformed,
                    TeamfightHealing = (uint?)player.Score.TeamfightHealingDone,
                    TeamfightDamageTaken = (uint?)player.Score.TeamfightDamageTaken,
                    TeamfightHeroDamage = (uint?)player.Score.TeamfightHeroDamage,
                    Multikill = (uint?)player.Score.Multikill,
                    PhysicalDamage = (uint?)player.Score.PhysicalDamage,
                    SpellDamage = (uint?)player.Score.SpellDamage,
                    RegenGlobes = (int?)player.Score.RegenGlobes,
                    GamesPlayed = 1
                };

                await _context.GlobalHeroStats.Upsert(globalHeroStats)
                              .WhenMatched(x => new GlobalHeroStats
                              {
                                  GameTime = x.GameTime + globalHeroStats.GameTime,
                                  Kills = x.Kills + globalHeroStats.Kills,
                                  Assists = x.Assists + globalHeroStats.Assists,
                                  Takedowns = x.Takedowns + globalHeroStats.Takedowns,
                                  Deaths = x.Deaths + globalHeroStats.Deaths,
                                  HighestKillStreak = x.HighestKillStreak + globalHeroStats.HighestKillStreak,
                                  HeroDamage = x.HeroDamage + globalHeroStats.HeroDamage,
                                  SiegeDamage = x.SiegeDamage + globalHeroStats.SiegeDamage,
                                  StructureDamage = x.StructureDamage + globalHeroStats.StructureDamage,
                                  MinionDamage = x.MinionDamage + globalHeroStats.MinionDamage,
                                  CreepDamage = x.CreepDamage + globalHeroStats.CreepDamage,
                                  SummonDamage = x.SummonDamage + globalHeroStats.SummonDamage,
                                  TimeCcEnemyHeroes = x.TimeCcEnemyHeroes + globalHeroStats.TimeCcEnemyHeroes,
                                  Healing = x.Healing + globalHeroStats.Healing,
                                  SelfHealing = x.SelfHealing + globalHeroStats.SelfHealing,
                                  DamageTaken = x.DamageTaken + globalHeroStats.DamageTaken,
                                  ExperienceContribution =
                                              x.ExperienceContribution + globalHeroStats.ExperienceContribution,
                                  TownKills = x.TownKills + globalHeroStats.TownKills,
                                  TimeSpentDead = x.TimeSpentDead + globalHeroStats.TimeSpentDead,
                                  MercCampCaptures = x.MercCampCaptures + globalHeroStats.MercCampCaptures,
                                  WatchTowerCaptures = x.WatchTowerCaptures + globalHeroStats.WatchTowerCaptures,
                                  ProtectionAllies = x.ProtectionAllies + globalHeroStats.ProtectionAllies,
                                  SilencingEnemies = x.SilencingEnemies + globalHeroStats.SilencingEnemies,
                                  RootingEnemies = x.RootingEnemies + globalHeroStats.RootingEnemies,
                                  StunningEnemies = x.StunningEnemies + globalHeroStats.StunningEnemies,
                                  ClutchHeals = x.ClutchHeals + globalHeroStats.ClutchHeals,
                                  Escapes = x.Escapes + globalHeroStats.Escapes,
                                  Vengeance = x.Vengeance + globalHeroStats.Vengeance,
                                  OutnumberedDeaths = x.OutnumberedDeaths + globalHeroStats.OutnumberedDeaths,
                                  TeamfightEscapes = x.TeamfightEscapes + globalHeroStats.TeamfightEscapes,
                                  TeamfightHealing = x.TeamfightHealing + globalHeroStats.TeamfightHealing,
                                  TeamfightDamageTaken =
                                              x.TeamfightDamageTaken + globalHeroStats.TeamfightDamageTaken,
                                  TeamfightHeroDamage =
                                              x.TeamfightHeroDamage + globalHeroStats.TeamfightHeroDamage,
                                  Multikill = x.Multikill + globalHeroStats.Multikill,
                                  PhysicalDamage = x.PhysicalDamage + globalHeroStats.PhysicalDamage,
                                  SpellDamage = x.SpellDamage + globalHeroStats.SpellDamage,
                                  RegenGlobes = x.RegenGlobes + globalHeroStats.RegenGlobes,
                                  GamesPlayed = x.GamesPlayed + globalHeroStats.GamesPlayed,
                              }).RunAsync();
            }
        }
        private async Task<int> InsertTalentCombo(string hero, int levelOne, int levelFour, int levelSeven, int levelTen, int levelThirteen, int levelSixteen, int levelTwenty)
        {
            var combo = new TalentCombinations
            {
                    Hero = Convert.ToInt32(hero),
                    LevelOne = levelOne,
                    LevelFour = levelFour,
                    LevelSeven = levelSeven,
                    LevelTen = levelTen,
                    LevelThirteen = levelThirteen,
                    LevelSixteen = levelSixteen,
                    LevelTwenty = levelTwenty
            };

            await _context.TalentCombinations.AddAsync(combo);
            await _context.SaveChangesAsync();

            return combo.TalentCombinationId;
        }

        private async Task<int> GetOrInsertHeroTalentComboId(string hero, int levelOne, int levelFour, int levelSeven, int levelTen, int levelThirteen, int levelSixteen, int levelTwenty)
        {
            var talentCombo = await _context.TalentCombinations.FirstOrDefaultAsync(x =>
                    x.Hero == Convert.ToInt32(hero)
                 && x.LevelOne == levelOne
                 && x.LevelFour == levelFour
                 && x.LevelSeven == levelSeven
                 && x.LevelTen == levelTen
                 && x.LevelThirteen == levelThirteen
                 && x.LevelSixteen == levelSixteen
                 && x.LevelTwenty == levelTwenty);

            var combId = talentCombo?.TalentCombinationId ?? await InsertTalentCombo(hero, levelOne, levelFour, levelSeven, levelTen, levelThirteen, levelSixteen, levelTwenty);

            return combId;
        }

        private async Task UpdateGlobalTalentData(ParsedStormReplay parsedStormReplay)
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

                int talentComboId;
                if (player.Talents == null)
                {
                    talentComboId = await GetOrInsertHeroTalentComboId(player.Hero_id, 0, 0, 0, 0, 0, 0, 0);
                }
                else
                {
                    var levelOne = player?.Talents?[0] == null || player.Talents[0] == ""
                            ? 0
                            : Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[0]]);
                    var levelFour = string.IsNullOrEmpty(player?.Talents[1])
                            ? 0
                            : Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[1]]);
                    var levelSeven = string.IsNullOrEmpty(player?.Talents[2])
                            ? 0
                            : Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[2]]);
                    var levelTen = string.IsNullOrEmpty(player?.Talents[3])
                            ? 0
                            : Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[3]]);
                    var levelThirteen = string.IsNullOrEmpty(player?.Talents[4])
                            ? 0
                            : Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[4]]);
                    var levelSixteen = string.IsNullOrEmpty(player?.Talents[5])
                            ? 0
                            : Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[5]]);
                    var levelTwenty = string.IsNullOrEmpty(player?.Talents[6])
                            ? 0
                            : Convert.ToInt32(parsedStormReplay.Talents[player.Hero + "|" + player.Talents[6]]);
                    talentComboId = await GetOrInsertHeroTalentComboId(player.Hero_id, levelOne, levelFour, levelSeven,
                            levelTen, levelThirteen, levelSixteen, levelTwenty);

                }

                var talent = new GlobalHeroTalents
                {
                        GameVersion = parsedStormReplay.OverallData.Version,
                        GameType = Convert.ToSByte(parsedStormReplay.OverallData.GameType_id),
                        LeagueTier = 0,
                        HeroLeagueTier = 0,
                        RoleLeagueTier = 0,
                        GameMap = Convert.ToSByte(parsedStormReplay.OverallData.GameMap_id),
                        HeroLevel = (uint) heroLevel,
                        Hero = Convert.ToSByte(player.Hero_id),
                        Mirror = (sbyte) player.Mirror,
                        Region = Convert.ToSByte(parsedStormReplay.OverallData.Region),
                        WinLoss = (sbyte) winLoss,
                        TalentCombinationId = talentComboId,
                        GameTime = (uint?) parsedStormReplay.OverallData.Length.UtcDateTime.TimeOfDay.TotalSeconds,
                        Kills = (uint?) player.Score.SoloKills,
                        Assists = (uint?) player.Score.Assists,
                        Takedowns = (uint?) player.Score.Takedowns,
                        Deaths = (uint?) player.Score.Deaths,
                        HighestKillStreak = (uint?) player.Score.HighestKillStreak,
                        HeroDamage = (uint?) player.Score.HeroDamage,
                        SiegeDamage = (uint?) player.Score.SiegeDamage,
                        StructureDamage = (uint?) player.Score.StructureDamage,
                        MinionDamage = (uint?) player.Score.MinionDamage,
                        CreepDamage = (uint?) player.Score.CreepDamage,
                        SummonDamage = (uint?) player.Score.SummonDamage,
                        TimeCcEnemyHeroes =
                                (uint?) player.Score.TimeCCdEnemyHeroes_not_null.UtcDateTime.TimeOfDay.TotalSeconds,
                        Healing = (uint?) player.Score.Healing,
                        SelfHealing = (uint?) player.Score.SelfHealing,
                        DamageTaken = (uint?) player.Score.DamageTaken,
                        ExperienceContribution = (uint?) player.Score.ExperienceContribution,
                        TownKills = (uint?) player.Score.TownKills,
                        TimeSpentDead = (uint?) player.Score.TimeSpentDead.UtcDateTime.TimeOfDay.TotalSeconds,
                        MercCampCaptures = (uint?) player.Score.MercCampCaptures,
                        WatchTowerCaptures = (uint?) player.Score.WatchTowerCaptures,
                        ProtectionAllies = (uint?) player.Score.ProtectionGivenToAllies,
                        SilencingEnemies = (uint?) player.Score.TimeSilencingEnemyHeroes,
                        RootingEnemies = (uint?) player.Score.TimeRootingEnemyHeroes,
                        StunningEnemies = (uint?) player.Score.TimeStunningEnemyHeroes,
                        ClutchHeals = (uint?) player.Score.ClutchHealsPerformed,
                        Escapes = (uint?) player.Score.EscapesPerformed,
                        Vengeance = (uint?) player.Score.VengeancesPerformed,
                        OutnumberedDeaths = (uint?) player.Score.OutnumberedDeaths,
                        TeamfightEscapes = (uint?) player.Score.TeamfightEscapesPerformed,
                        TeamfightHealing = (uint?) player.Score.TeamfightHealingDone,
                        TeamfightDamageTaken = (uint?) player.Score.TeamfightDamageTaken,
                        TeamfightHeroDamage = (uint?) player.Score.TeamfightHeroDamage,
                        Multikill = (uint?) player.Score.Multikill,
                        PhysicalDamage = (uint?) player.Score.PhysicalDamage,
                        SpellDamage = (uint?) player.Score.SpellDamage,
                        RegenGlobes = (int?) player.Score.RegenGlobes,
                        GamesPlayed = 1,
                };

                await _context.GlobalHeroTalents.Upsert(talent)
                              .WhenMatched(x => new GlobalHeroTalents
                              {
                                      GameTime = x.GameTime + talent.GamesPlayed,
                                      Kills = x.Kills + talent.Kills,
                                      Assists = x.Assists + talent.Assists,
                                      Takedowns = x.Takedowns + talent.Takedowns,
                                      Deaths = x.Deaths + talent.Deaths,
                                      HighestKillStreak = x.HighestKillStreak + talent.HighestKillStreak,
                                      HeroDamage = x.HeroDamage + talent.HeroDamage,
                                      SiegeDamage = x.SiegeDamage + talent.SiegeDamage,
                                      StructureDamage = x.StructureDamage + talent.StructureDamage,
                                      MinionDamage = x.MinionDamage + talent.MinionDamage,
                                      CreepDamage = x.CreepDamage + talent.CreepDamage,
                                      SummonDamage = x.SummonDamage + talent.SummonDamage,
                                      TimeCcEnemyHeroes = x.TimeCcEnemyHeroes + talent.TimeCcEnemyHeroes,
                                      Healing = x.Healing + talent.Healing,
                                      SelfHealing = x.SelfHealing + talent.SelfHealing,
                                      DamageTaken = x.DamageTaken + talent.DamageTaken,
                                      ExperienceContribution =
                                              x.ExperienceContribution + talent.ExperienceContribution,
                                      TownKills = x.TownKills + talent.TownKills,
                                      TimeSpentDead = x.TimeSpentDead + talent.TimeSpentDead,
                                      MercCampCaptures = x.MercCampCaptures + talent.MercCampCaptures,
                                      WatchTowerCaptures = x.WatchTowerCaptures + talent.WatchTowerCaptures,
                                      ProtectionAllies = x.ProtectionAllies + talent.ProtectionAllies,
                                      SilencingEnemies = x.SilencingEnemies + talent.SilencingEnemies,
                                      RootingEnemies = x.RootingEnemies + talent.RootingEnemies,
                                      StunningEnemies = x.StunningEnemies + talent.StunningEnemies,
                                      ClutchHeals = x.ClutchHeals + talent.ClutchHeals,
                                      Escapes = x.Escapes + talent.Escapes,
                                      Vengeance = x.Vengeance + talent.Vengeance,
                                      OutnumberedDeaths = x.OutnumberedDeaths + talent.OutnumberedDeaths,
                                      TeamfightEscapes = x.TeamfightEscapes + talent.TeamfightEscapes,
                                      TeamfightHealing = x.TeamfightHealing + talent.TeamfightHealing,
                                      TeamfightDamageTaken = x.TeamfightDamageTaken + talent.TeamfightDamageTaken,
                                      TeamfightHeroDamage = x.TeamfightHeroDamage + talent.TeamfightHeroDamage,
                                      Multikill = x.Multikill + talent.Multikill,
                                      PhysicalDamage = x.PhysicalDamage + talent.PhysicalDamage,
                                      SpellDamage = x.SpellDamage + talent.SpellDamage,
                                      RegenGlobes = x.RegenGlobes + talent.RegenGlobes,
                                      GamesPlayed = x.GamesPlayed + talent.GamesPlayed,
                              }).RunAsync();
            }
        }

        private async Task UpdateGameModeTotalGames(int season, ParsedStormReplay parsedStormReplay)
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

                await UpsertMasterGamesPlayedData(Convert.ToInt32(parsedStormReplay.MmrIds["player"]), season,
                        Convert.ToByte(parsedStormReplay.OverallData.GameType_id),
                        (uint) player.BlizzId, (byte) parsedStormReplay.OverallData.Region, wins, losses, 1);

                await UpsertMasterGamesPlayedData(
                        Convert.ToInt32(parsedStormReplay.MmrIds[parsedStormReplay.Role[player.Hero]]), season,
                        Convert.ToByte(parsedStormReplay.OverallData.GameType_id),
                        (uint) player.BlizzId, (byte) parsedStormReplay.OverallData.Region, wins, losses, 1);

                await UpsertMasterGamesPlayedData(Convert.ToInt32(parsedStormReplay.MmrIds[player.Hero]), season,
                        Convert.ToByte(parsedStormReplay.OverallData.GameType_id),
                        (uint) player.BlizzId, (byte) parsedStormReplay.OverallData.Region, wins, losses, 1);
            }
        }

        private async Task UpdateGlobalTalentDataDetails(ParsedStormReplay parsedStormReplay)
        {
            for (var i = 0; i < parsedStormReplay.OverallData.Players.Length; i++)
            {
                for (var j = 0; j < parsedStormReplay.OverallData.Players.Length; j++)
                {
                    if (j == i) continue;
                    if (parsedStormReplay.OverallData.Players[i].Hero !=
                        parsedStormReplay.OverallData.Players[j].Hero) continue;
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

                    var talentDetail = new GlobalHeroTalentsDetails
                    {
                            GameVersion = parsedStormReplay.OverallData.Version,
                            GameType = Convert.ToSByte(parsedStormReplay.OverallData.GameType_id),
                            LeagueTier = 0,
                            HeroLeagueTier = 0,
                            RoleLeagueTier = 0,
                            GameMap = Convert.ToSByte(parsedStormReplay.OverallData.GameMap_id),
                            HeroLevel = (uint) heroLevel,
                            Hero = Convert.ToSByte(player.Hero_id),
                            Mirror = (sbyte) player.Mirror,
                            Region = Convert.ToSByte(parsedStormReplay.OverallData.Region),
                            WinLoss = (sbyte) winLoss,
                            Level = Convert.ToInt32(level),
                            GameTime = (uint?) parsedStormReplay.OverallData.Length.UtcDateTime.TimeOfDay.TotalSeconds,
                            Kills = (uint?) player.Score.SoloKills,
                            Assists = (uint?) player.Score.Assists,
                            Takedowns = (uint?) player.Score.Takedowns,
                            Deaths = (uint?) player.Score.Deaths,
                            HighestKillStreak = (uint?) player.Score.HighestKillStreak,
                            HeroDamage = (uint?) player.Score.HeroDamage,
                            SiegeDamage = (uint?) player.Score.SiegeDamage,
                            StructureDamage = (uint?) player.Score.StructureDamage,
                            MinionDamage = (uint?) player.Score.MinionDamage,
                            CreepDamage = (uint?) player.Score.CreepDamage,
                            SummonDamage = (uint?) player.Score.SummonDamage,
                            TimeCcEnemyHeroes =
                                    (uint?) player.Score.TimeCCdEnemyHeroes_not_null.UtcDateTime.TimeOfDay.TotalSeconds,
                            Healing = (uint?) player.Score.Healing,
                            SelfHealing = (uint?) player.Score.SelfHealing,
                            DamageTaken = (uint?) player.Score.DamageTaken,
                            ExperienceContribution = (uint?) player.Score.ExperienceContribution,
                            TownKills = (uint?) player.Score.TownKills,
                            TimeSpentDead = (uint?) player.Score.TimeSpentDead.UtcDateTime.TimeOfDay.TotalSeconds,
                            MercCampCaptures = (uint?) player.Score.MercCampCaptures,
                            WatchTowerCaptures = (uint?) player.Score.WatchTowerCaptures,
                            ProtectionAllies = (uint?) player.Score.ProtectionGivenToAllies,
                            SilencingEnemies = (uint?) player.Score.TimeSilencingEnemyHeroes,
                            RootingEnemies = (uint?) player.Score.TimeRootingEnemyHeroes,
                            StunningEnemies = (uint?) player.Score.TimeStunningEnemyHeroes,
                            ClutchHeals = (uint?) player.Score.ClutchHealsPerformed,
                            Escapes = (uint?) player.Score.EscapesPerformed,
                            Vengeance = (uint?) player.Score.VengeancesPerformed,
                            OutnumberedDeaths = (uint?) player.Score.OutnumberedDeaths,
                            TeamfightEscapes = (uint?) player.Score.TeamfightEscapesPerformed,
                            TeamfightHealing = (uint?) player.Score.TeamfightHealingDone,
                            TeamfightDamageTaken = (uint?) player.Score.TeamfightDamageTaken,
                            TeamfightHeroDamage = (uint?) player.Score.TeamfightHeroDamage,
                            Multikill = (uint?) player.Score.Multikill,
                            PhysicalDamage = (uint?) player.Score.PhysicalDamage,
                            SpellDamage = (uint?) player.Score.SpellDamage,
                            RegenGlobes = (int?) player.Score.RegenGlobes,
                            GamesPlayed = 1,
                            Talent = t switch
                            {
                                    0 => Convert.ToInt32(
                                            parsedStormReplay.Talents[player.Hero + "|" + player.Talents[0]]),
                                    1 => Convert.ToInt32(
                                            parsedStormReplay.Talents[player.Hero + "|" + player.Talents[1]]),
                                    2 => Convert.ToInt32(
                                            parsedStormReplay.Talents[player.Hero + "|" + player.Talents[2]]),
                                    3 => Convert.ToInt32(
                                            parsedStormReplay.Talents[player.Hero + "|" + player.Talents[3]]),
                                    4 => Convert.ToInt32(
                                            parsedStormReplay.Talents[player.Hero + "|" + player.Talents[4]]),
                                    5 => Convert.ToInt32(
                                            parsedStormReplay.Talents[player.Hero + "|" + player.Talents[5]]),
                                    6 => Convert.ToInt32(
                                            parsedStormReplay.Talents[player.Hero + "|" + player.Talents[6]])
                            }
                    };


                    await _context.GlobalHeroTalentsDetails.Upsert(talentDetail)
                                  .WhenMatched(x => new GlobalHeroTalentsDetails
                                  {
                                          GameTime = x.GameTime + talentDetail.GameTime,
                                          Kills = x.Kills + talentDetail.Kills,
                                          Assists = x.Assists + talentDetail.Assists,
                                          Takedowns = x.Takedowns + talentDetail.Takedowns,
                                          Deaths = x.Deaths + talentDetail.Deaths,
                                          HighestKillStreak = x.HighestKillStreak + talentDetail.HighestKillStreak,
                                          HeroDamage = x.HeroDamage + talentDetail.HeroDamage,
                                          SiegeDamage = x.SiegeDamage + talentDetail.SiegeDamage,
                                          StructureDamage = x.StructureDamage + talentDetail.StructureDamage,
                                          MinionDamage = x.MinionDamage + talentDetail.MinionDamage,
                                          CreepDamage = x.CreepDamage + talentDetail.CreepDamage,
                                          SummonDamage = x.SummonDamage + talentDetail.SummonDamage,
                                          TimeCcEnemyHeroes = x.TimeCcEnemyHeroes + talentDetail.TimeCcEnemyHeroes,
                                          Healing = x.Healing + talentDetail.Healing,
                                          SelfHealing = x.SelfHealing + talentDetail.SelfHealing,
                                          DamageTaken = x.DamageTaken + talentDetail.DamageTaken,
                                          ExperienceContribution =
                                                  x.ExperienceContribution + talentDetail.ExperienceContribution,
                                          TownKills = x.TownKills + talentDetail.TownKills,
                                          TimeSpentDead = x.TimeSpentDead + talentDetail.TimeSpentDead,
                                          MercCampCaptures = x.MercCampCaptures + talentDetail.MercCampCaptures,
                                          WatchTowerCaptures = x.WatchTowerCaptures + talentDetail.WatchTowerCaptures,
                                          ProtectionAllies = x.ProtectionAllies + talentDetail.ProtectionAllies,
                                          SilencingEnemies = x.SilencingEnemies + talentDetail.SilencingEnemies,
                                          RootingEnemies = x.RootingEnemies + talentDetail.RootingEnemies,
                                          StunningEnemies = x.StunningEnemies + talentDetail.StunningEnemies,
                                          ClutchHeals = x.ClutchHeals + talentDetail.ClutchHeals,
                                          Escapes = x.Escapes + talentDetail.Escapes,
                                          Vengeance = x.Vengeance + talentDetail.Vengeance,
                                          OutnumberedDeaths = x.OutnumberedDeaths + talentDetail.OutnumberedDeaths,
                                          TeamfightEscapes = x.TeamfightEscapes + talentDetail.TeamfightEscapes,
                                          TeamfightHealing = x.TeamfightHealing + talentDetail.TeamfightHealing,
                                          TeamfightDamageTaken =
                                                  x.TeamfightDamageTaken + talentDetail.TeamfightDamageTaken,
                                          TeamfightHeroDamage =
                                                  x.TeamfightHeroDamage + talentDetail.TeamfightHeroDamage,
                                          Multikill = x.Multikill + talentDetail.Multikill,
                                          PhysicalDamage = x.PhysicalDamage + talentDetail.PhysicalDamage,
                                          SpellDamage = x.SpellDamage + talentDetail.SpellDamage,
                                          RegenGlobes = x.RegenGlobes + talentDetail.RegenGlobes,
                                          GamesPlayed = x.GamesPlayed + talentDetail.GamesPlayed,
                                  }).RunAsync();
                }
            }
        }

        public async Task<string> InsertIntoTalentTable(string hero, string talentName, Dictionary<string, string> heroesAlt)
        {
            if (hero == "")
            {
                var split = Regex.Split(talentName, @"(?<!^)(?=[A-Z])");
                hero = heroesAlt.ContainsKey(split[0]) ? heroesAlt[split[0]] : split[0];
            }

            var existing = await _context.HeroesDataTalents.FirstOrDefaultAsync(x => x.HeroName == hero &&
                                                                        x.TalentName == talentName);
            if (existing != null) return existing.TalentId.ToString();

            var talents = new HeroesDataTalents
            {
                    HeroName = hero,
                    TalentName = talentName,
                    ShortName = "",
                    AttributeId = "",
                    Title = "",
                    Description = "",
                    Status = "",
                    Hotkey = "",
                    Cooldown = "",
                    ManaCost = "",
                    Sort = "",
                    Icon = ""

            };

            await _context.HeroesDataTalents.AddAsync(talents);
            await _context.SaveChangesAsync();
            return talents.TalentId.ToString();

        }

        private async Task InsertUrlIntoReplayUrls(ParsedStormReplay parsedStormReplay)
        {
            await _context.ReplayUrls.AddAsync(new ReplayUrls
            {
                    ReplayId = (int) parsedStormReplay.ReplayId,
                    GameDate = Convert.ToDateTime(parsedStormReplay.OverallData.Date),
                    GameType = Convert.ToSByte(parsedStormReplay.OverallData.GameType_id),
                    Url = parsedStormReplay.ReplayUrl.ToString()
            });
            await _context.SaveChangesAsync();
        }

        private async Task<string> SaveToSeasonGameVersion(DateTime gameDate, string gameVersion, ParsedStormReplay parsedStormReplay)
        {
            var season = "";

            foreach (var s in parsedStormReplay.Seasons.Keys.Where(s => gameDate >= parsedStormReplay.Seasons[s][0] && gameDate < parsedStormReplay.Seasons[s][1]))
            {
                season = s;
            }

            var existing = await _context.SeasonGameVersions.FirstOrDefaultAsync(x => x.Season == Convert.ToInt32(season) &&
                                                                                     x.GameVersion == gameVersion &&
                                                                                     x.DateAdded == DateTime.Now);
            if (existing == null)
            {
                var version = new SeasonGameVersions
                {
                        Season = Convert.ToInt32(season),
                        GameVersion = gameVersion,
                        DateAdded = DateTime.Now
                };

                await _context.SeasonGameVersions.AddAsync(version);
                await _context.SaveChangesAsync();
            }
            return season;
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

        public async Task InsertNotProcessedReplay(int replayId, string parsedId,
            int? region, string gameType, string gameLength, DateTime gameDate,
            string map, string version, string size, DateTime? dateParsed,
            int? countParsed, string url, string processed, string failureStatus)

        {
            var row = new HeroesProfileDb.HeroesProfile.ReplaysNotProcessed
            {
                ReplayId = replayId,
                ParsedId = parsedId,
                Region = region,
                GameType = gameType,
                GameLength = gameLength,
                GameDate = gameDate,
                GameMap = map,
                GameVersion = version,
                Size = size,
                DateParsed = dateParsed,
                CountParsed = countParsed,
                Url = url,
                Processed = processed,
                FailureStatus = failureStatus
            };
            await _context.ReplaysNotProcessed.BulkInsertAsync(
                new List<HeroesProfileDb.HeroesProfile.ReplaysNotProcessed>()
                {
                    row
                }, o => { o.InsertIfNotExists = true; });
            await _context.BulkSaveChangesAsync();
        }

        public async Task UpsertNotProcessedReplay(int replayId, string parsedId,
            int? region, string gameType, string gameLength, DateTime gameDate,
            string map, string version, string size, DateTime? dateParsed,
            int? countParsed, string url, string processed, string failureStatus)

        {
            var row = new HeroesProfileDb.HeroesProfile.ReplaysNotProcessed
            {
                ReplayId = replayId,
                ParsedId = parsedId,
                Region = region,
                GameType = gameType,
                GameLength = gameLength,
                GameDate = gameDate,
                GameMap = map,
                GameVersion = version,
                Size = size,
                DateParsed = dateParsed,
                CountParsed = countParsed,
                Url = url,
                Processed = processed,
                FailureStatus = failureStatus
            };
            await _context.ReplaysNotProcessed.Upsert(row)
                .WhenMatched(x => new HeroesProfileDb.HeroesProfile.ReplaysNotProcessed
                {
                    ReplayId = row.ReplayId,
                    ParsedId = row.ParsedId,
                    Region = row.Region,
                    GameType = row.GameType,
                    GameLength = row.GameLength,
                    GameDate = row.GameDate,
                    GameMap = row.GameMap,
                    GameVersion = row.GameVersion,
                    Size = row.Size,
                    DateParsed = row.DateParsed,
                    CountParsed = row.CountParsed,
                    Url = row.Url,
                    Processed = row.Processed,
                    FailureStatus = row.FailureStatus
                })
                .RunAsync();
        }

        public async Task UpsertNotProcessedReplayIncrementCountParsed(int replayId, string parsedId,
                                                   int? region, string gameType, string gameLength, DateTime gameDate,
                                                   string map, string version, string size, DateTime? dateParsed,
                                                   int? countParsed, string url, string processed, string failureStatus)

        {
            var row = new HeroesProfileDb.HeroesProfile.ReplaysNotProcessed
            {
                    ReplayId = replayId,
                    ParsedId = parsedId,
                    Region = region,
                    GameType = gameType,
                    GameLength = gameLength,
                    GameDate = gameDate,
                    GameMap = map,
                    GameVersion = version,
                    Size = size,
                    DateParsed = dateParsed,
                    CountParsed = countParsed,
                    Url = url,
                    Processed = processed,
                    FailureStatus = failureStatus
            };
            await _context.ReplaysNotProcessed.Upsert(row)
                          .WhenMatched(x => new HeroesProfileDb.HeroesProfile.ReplaysNotProcessed
                          {
                                  CountParsed = x.CountParsed == null ? 1 : x.CountParsed + 1
                          })
                          .RunAsync();
        }

        public async Task UpsertMasterGamesPlayedData(int typeValue, double season, byte gameType, uint blizzId, byte region, int? win, int? loss, int? gamesPlayed)
        {
            var row = new MasterGamesPlayedData
            {
                    TypeValue = typeValue,
                    Season = season,
                    GameType = gameType,
                    BlizzId = blizzId,
                    Region = region,
                    Win = win, 
                    Loss = loss,
                    GamesPlayed = gamesPlayed
            };
            await _context.MasterGamesPlayedData.Upsert(row)
                          .WhenMatched(x => new MasterGamesPlayedData
                          {
                                  TypeValue = row.TypeValue,
                                  Season = row.Season,
                                  GameType = row.GameType,
                                  BlizzId = row.BlizzId,
                                  Region = row.Region,
                                  Win = x.Win + row.Win,
                                  Loss = x.Loss + row.Loss,
                                  GamesPlayed = x.GamesPlayed + row.GamesPlayed
                          })
                          .RunAsync();
        }
    }
}
