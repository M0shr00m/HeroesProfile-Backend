using System;
using System.Collections.Generic;

namespace HeroesProfile_Backend.Models
{
    public class ParsedStormReplay
    {
        public LambdaReplayData Data = new LambdaReplayData();

        public Dictionary<string, string> Heroes = new Dictionary<string, string>();
        public Dictionary<string, string> HeroesAlt = new Dictionary<string, string>();
        public Dictionary<string, string> Role = new Dictionary<string, string>();
        public Dictionary<string, string> Maps = new Dictionary<string, string>();
        public Dictionary<string, string> MapsTranslations = new Dictionary<string, string>();
        public Dictionary<string, string> GameTypes = new Dictionary<string, string>();
        public Dictionary<string, string> Talents = new Dictionary<string, string>();
        public Dictionary<string, string> SeasonsGameVersions = new Dictionary<string, string>();
        public Dictionary<string, string> MmrIds = new Dictionary<string, string>();
        public Dictionary<string, DateTime[]> Seasons = new Dictionary<string, DateTime[]>();
        public Dictionary<string, string> HeroesTranslations = new Dictionary<string, string>();
        public Dictionary<string, string> MapsShort = new Dictionary<string, string>();
        public Dictionary<string, string> HeroesAttr = new Dictionary<string, string>();

        public long ReplayId;
        public Uri ReplayUrl;

        public bool Dupe = false;
        public LambdaJson.ReplayData OverallData;
    }
}