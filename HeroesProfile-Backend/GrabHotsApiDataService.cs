﻿using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using System.Net;
using System.IO;
using System.Text.RegularExpressions;
using HeroesProfile_Backend.Models;
using HeroesProfileDb.HeroesProfile;
using HeroesProfileDb.HeroesProfileBrawl;
using Microsoft.EntityFrameworkCore;

namespace HeroesProfile_Backend
{
     public class GrabHotsApiDataService
     {
         private readonly HeroesProfileContext _context;
         private readonly HeroesProfileBrawlContext _brawlContext;
         private readonly ApiSettings _apiSettings;
         private ParseStormReplayService _parseStormReplayService;
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
        private Dictionary<string, DateTime[]> _seasons = new Dictionary<string, DateTime[]>();
        private Dictionary<string, string> _seasonsGameVersions = new Dictionary<string, string>();
        private Dictionary<string, Models.ReplaysNotProcessed> _notProcessedReplays = new Dictionary<string, Models.ReplaysNotProcessed>();
        private Dictionary<long, HotsApiJSON.ReplayData> _replaysToRun = new Dictionary<long, HotsApiJSON.ReplayData>();
        private ConcurrentDictionary<long, ParsedStormReplay> _replayDataGrabbed = new ConcurrentDictionary<long, ParsedStormReplay>();

        public GrabHotsApiDataService(ParseStormReplayService parseStormReplayService, ApiSettings apiSettings,
                                      HeroesProfileContext context, HeroesProfileBrawlContext brawlContext)
        {
            _parseStormReplayService = parseStormReplayService;
            _apiSettings = apiSettings;
            _context = context;
            _brawlContext = brawlContext;
        }
        
        public async Task GrabHotsApiData()
        {
            var maxValue = 0;

            var heroes = await _context.Heroes.Select(x => new {x.Id, x.Name, x.NewRole, x.AltName, x.ShortName, x.AttributeId}).ToListAsync();

            foreach (var hero in heroes)
            {
                _heroes.Add(hero.Name, hero.Id.ToString());
                _role.Add(hero.Name, hero.NewRole);

                if (!_heroesAlt.ContainsKey(hero.Name))
                {
                    var alt = hero.AltName;

                    if (alt == "")
                    {
                        alt = hero.ShortName;
                        alt = char.ToUpper(alt.First()) + alt.Substring(1).ToLower();

                    }
                    _heroesAlt.Add(alt, hero.Name);
                    _heroesAttr.Add(hero.AttributeId, hero.Name);
                }
            }

            _gameTypes = (await _context.GameTypes.Select(x => new {x.TypeId, x.Name}).ToListAsync())
                    .ToDictionary(x => x.Name.Replace(" ", ""), x => x.TypeId.ToString());

            var heroesTranslations = await _context.HeroesTranslations.Select(x => new { x.Translation, x.Name}).ToListAsync();

            foreach (var translation in heroesTranslations.Where(translation => !_heroesTranslations.ContainsKey(translation.Translation.ToLower())))
            {
                _heroesTranslations.Add(translation.Translation.ToLower(), translation.Name);
            }

            var maps = await _context.Maps.Select(x => new { x.MapId, x.Name, x.ShortName }).ToListAsync();

            foreach (var map in maps)
            {
                _maps.Add(map.Name, map.MapId.ToString());
                _mapsShort.Add(map.ShortName, map.Name);
            }

            _mapsTranslations = (await _context.MapsTranslations.Select(x => new {x.Translation, x.Name}).ToListAsync())
                    .ToDictionary(x => x.Translation, x => x.Name);


            var heroesDataTalents = await _context.HeroesDataTalents.Select(x => new {x.HeroName, x.TalentId, x.TalentName})
                                            .OrderBy(x => x.TalentId)
                                            .ToListAsync();

            foreach (var talent in heroesDataTalents.Where(talent => !_talents.ContainsKey(talent.HeroName + "|" + talent.TalentName)))
            {
                _talents.Add(talent.HeroName + "|" + talent.TalentName, talent.TalentId.ToString());
            }

            _mmrIds = (await _context.MmrTypeIds.Select(x => new { x.MmrTypeId, x.Name }).ToListAsync())
                    .ToDictionary(x => x.Name, x => x.MmrTypeId.ToString());

            var seasonDates = await _context.SeasonDates.Select(x => new {x.Id, x.StartDate, x.EndDate}).ToListAsync();

            foreach (var seasonDate in seasonDates)
            {
                var dates = new DateTime[2];

                dates[0] = seasonDate.StartDate;
                dates[1] = seasonDate.EndDate;

                _seasons.Add(seasonDate.Id.ToString(), dates);
            }

            var replaysNotProcessed = await _context.ReplaysNotProcessed.Where(x => x.CountParsed < 3)
                                              .OrderBy(x => x.ReplayId)
                                              .Take(100)
                                              .ToListAsync();

            foreach (var replay in replaysNotProcessed)
            {
                var ron = new Models.ReplaysNotProcessed
                {
                    replayID = replay.ReplayId.ToString(),
                    region = replay.Region.ToString(),
                    game_type = replay.GameType,
                    game_length = replay.GameLength,
                    game_date = replay.GameDate.ToString(),
                    game_map = replay.GameMap,
                    game_version = replay.GameVersion,
                    size = replay.Size,
                    date_parsed = replay.DateParsed.ToString(),
                    count_parsed = replay.CountParsed?.ToString() ?? "",
                    url = replay.Url,
                    failure_status = replay.FailureStatus,
                    processed = replay.Processed
                };
                _notProcessedReplays.Add(replay.ReplayId.ToString(), ron);
            }

            var seasonGameVersions = await _context.SeasonGameVersions.Select(x => new { x.Season, x.GameVersion }).ToListAsync();

            foreach (var version in seasonGameVersions)
            {
                if (!_seasonsGameVersions.ContainsKey(version.GameVersion))
                {
                    _seasonsGameVersions.Add(version.GameVersion, version.Season.ToString());
                }
            }

            maxValue = (int) await _context.Replay.MaxAsync(x => x.ReplayId);

            RunNotProcessed();

            var notProcessedMaxValue = await _context.ReplaysNotProcessed.MaxAsync(x => x.ReplayId);

            var brawlMaxValue = await _brawlContext.Replay.MaxAsync(x => x.ReplayId);
            
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
                new ParallelOptions { MaxDegreeOfParallelism = 100 },
                item =>
                {
                    Console.WriteLine("Running Replay: " + item);
                    
                    var p = _parseStormReplayService.ParseStormReplay(Convert.ToInt64(item), new Uri(_notProcessedReplays[item].url, UriKind.Absolute), _notProcessedReplays[item], _maps, _mapsTranslations, _gameTypes, _talents, _seasonsGameVersions, _mmrIds, _seasons, _heroes, _heroesTranslations, _mapsShort, _mmrIds, _role, _heroesAttr);
                    _replayDataGrabbed.TryAdd(Convert.ToInt64(item), p);

                });

            var sortedReplayDataGrabbed = new SortedDictionary<long, ParsedStormReplay>();

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
                    if (sortedReplayDataGrabbed[item].Dupe) return;
                    if (sortedReplayDataGrabbed[item].OverallData == null) return;
                    if (sortedReplayDataGrabbed[item].OverallData.Mode == null) return;
                    Console.WriteLine("Saving replay data for: " + item);
                    _parseStormReplayService.SaveReplayData(sortedReplayDataGrabbed[item], isBrawl: sortedReplayDataGrabbed[item].OverallData.Mode == "Brawl");
                });


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
                    var p = _parseStormReplayService.ParseStormReplay(item, _replaysToRun[item].Url, _replaysToRun[item], _maps, _mapsTranslations, _gameTypes, _talents, _seasonsGameVersions, _mmrIds, _seasons, _heroes, _heroesTranslations, _mapsShort, _mmrIds, _role, _heroesAttr);
                    _replayDataGrabbed.TryAdd(item, p);
                });
                var sortedReplayDataGrabbed = new SortedDictionary<long, ParsedStormReplay>();

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
                        if (sortedReplayDataGrabbed[item].Dupe) return;
                        if (sortedReplayDataGrabbed[item].OverallData == null) return;
                        if (sortedReplayDataGrabbed[item].OverallData.Mode == null) return;
                        Console.WriteLine("Saving replay data for: " + item);
                            _parseStormReplayService.SaveReplayData(sortedReplayDataGrabbed[item], isBrawl: sortedReplayDataGrabbed[item].OverallData.Mode == "Brawl");

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
