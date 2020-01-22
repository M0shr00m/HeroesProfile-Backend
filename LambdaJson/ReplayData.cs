using System;
using System.Globalization;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace LambdaJson
{
 public partial class ReplayData
    {
        [JsonProperty("mode")]
        public string Mode { get; set; }
        public string GameType_id { get; set; }

        [JsonProperty("region")]
        public long? Region { get; set; }

        [JsonProperty("date")]
        public DateTimeOffset Date { get; set; }

        [JsonProperty("length")]
        public DateTimeOffset Length { get; set; }

        [JsonProperty("map")]
        public string Map { get; set; }
        public string GameMap_id { get; set; }

        [JsonProperty("map_short")]
        public string MapShort { get; set; }

        [JsonProperty("version")]
        public string Version { get; set; }

        [JsonProperty("version_major")]
        public long VersionMajor { get; set; }

        [JsonProperty("version_build")]
        public long VersionBuild { get; set; }

        [JsonProperty("bans")]
        public object[][] Bans { get; set; }

        [JsonProperty("draft_order")]
        public object[] DraftOrder { get; set; }

        [JsonProperty("team_experience")]
        public TeamExperience[][] TeamExperience { get; set; }

        [JsonProperty("players")]
        public Player[] Players { get; set; }
    }

    public partial class Player
    {
        //MMR Data
        public double player_conservative_rating { get; set; }
        public double player_mean { get; set; }
        public double player_standard_deviation { get; set; }

        public double role_conservative_rating { get; set; }
        public double role_mean { get; set; }
        public double role_standard_deviation { get; set; }

        public double hero_conservative_rating { get; set; }
        public double hero_mean { get; set; }
        public double hero_standard_deviation { get; set; }

        public string player_league_tier { get; set; }
        public string hero_league_tier { get; set; }
        public string role_league_tier { get; set; }

        [JsonProperty("battletag_name")]
        public string BattletagName { get; set; }

        [JsonProperty("battletag_id")]
        public long BattletagId { get; set; }
        public string battletag_table_id { get; set; }

        [JsonProperty("blizz_id")]
        public long BlizzId { get; set; }

        [JsonProperty("hero")]
        public string Hero { get; set; }
        public int Mirror { get; set; }
        public string Hero_id { get; set; }

        [JsonProperty("account_level")]
        public long AccountLevel { get; set; }

        [JsonProperty("hero_level")]
        public long HeroLevel { get; set; }

        [JsonProperty("hero_level_taunt")]
        public HeroLevelTaunt[] HeroLevelTaunt { get; set; }
        public long MasteyTauntTier { get; set; }

        [JsonProperty("team")]
        public long Team { get; set; }

        [JsonProperty("winner")]
        public bool Winner { get; set; }
        public string WinnerValue { get; set; }

        [JsonProperty("silenced")]
        public bool Silenced { get; set; }

        [JsonProperty("party")]
        public long? Party { get; set; }

        [JsonProperty("talents")]
        public string[] Talents { get; set; }

        [JsonProperty("score")]
        public Score Score { get; set; }

        [JsonProperty("staff")]
        public bool Staff { get; set; }
    }

    public partial class HeroLevelTaunt
    {
        [JsonProperty("HeroAttributeId")]
        public string HeroAttributeId { get; set; }

        [JsonProperty("TierLevel")]
        public long TierLevel { get; set; }
    }

    public partial class Score
    {
        [JsonProperty("Level")]
        public long? Level { get; set; }

        [JsonProperty("Takedowns")]
        public long? Takedowns { get; set; }

        [JsonProperty("SoloKills")]
        public long? SoloKills { get; set; }

        [JsonProperty("Assists")]
        public long? Assists { get; set; }

        [JsonProperty("Deaths")]
        public long? Deaths { get; set; }

        [JsonProperty("RegenGlobes")]
        public long RegenGlobes { get; set; }

        public long FirstToTen { get; set; }

        [JsonProperty("HeroDamage")]
        public long? HeroDamage { get; set; }

        [JsonProperty("SiegeDamage")]
        public long? SiegeDamage { get; set; }

        [JsonProperty("StructureDamage")]
        public long? StructureDamage { get; set; }

        [JsonProperty("MinionDamage")]
        public long? MinionDamage { get; set; }

        [JsonProperty("CreepDamage")]
        public long? CreepDamage { get; set; }

        [JsonProperty("SummonDamage")]
        public long? SummonDamage { get; set; }

        [JsonProperty("TimeCCdEnemyHeroes")]
        public string TimeCCdEnemyHeroes { get; set; }
        public DateTimeOffset TimeCCdEnemyHeroes_not_null { get; set; }

        [JsonProperty("Healing")]
        public long? Healing { get; set; }

        [JsonProperty("SelfHealing")]
        public long? SelfHealing { get; set; }

        [JsonProperty("DamageTaken")]
        public long? DamageTaken { get; set; }

        [JsonProperty("DamageSoaked")]
        public long? DamageSoaked { get; set; }

        [JsonProperty("ExperienceContribution")]
        public long? ExperienceContribution { get; set; }

        [JsonProperty("TownKills")]
        public long? TownKills { get; set; }

        [JsonProperty("TimeSpentDead")]
        public DateTimeOffset TimeSpentDead { get; set; }

        [JsonProperty("MercCampCaptures")]
        public long? MercCampCaptures { get; set; }

        [JsonProperty("WatchTowerCaptures")]
        public long? WatchTowerCaptures { get; set; }

        [JsonProperty("MetaExperience")]
        public long? MetaExperience { get; set; }

        [JsonProperty("HighestKillStreak")]
        public long? HighestKillStreak { get; set; }

        [JsonProperty("ProtectionGivenToAllies")]
        public long? ProtectionGivenToAllies { get; set; }

        [JsonProperty("TimeSilencingEnemyHeroes")]
        public long? TimeSilencingEnemyHeroes { get; set; }

        [JsonProperty("TimeRootingEnemyHeroes")]
        public long? TimeRootingEnemyHeroes { get; set; }

        [JsonProperty("TimeStunningEnemyHeroes")]
        public long? TimeStunningEnemyHeroes { get; set; }

        [JsonProperty("ClutchHealsPerformed")]
        public long? ClutchHealsPerformed { get; set; }

        [JsonProperty("EscapesPerformed")]
        public long? EscapesPerformed { get; set; }

        [JsonProperty("VengeancesPerformed")]
        public long? VengeancesPerformed { get; set; }

        [JsonProperty("OutnumberedDeaths")]
        public long? OutnumberedDeaths { get; set; }

        [JsonProperty("TeamfightEscapesPerformed")]
        public long? TeamfightEscapesPerformed { get; set; }

        [JsonProperty("TeamfightHealingDone")]
        public long? TeamfightHealingDone { get; set; }

        [JsonProperty("TeamfightDamageTaken")]
        public long? TeamfightDamageTaken { get; set; }

        [JsonProperty("TeamfightHeroDamage")]
        public long? TeamfightHeroDamage { get; set; }

        [JsonProperty("Multikill")]
        public long? Multikill { get; set; }

        [JsonProperty("PhysicalDamage")]
        public long? PhysicalDamage { get; set; }

        [JsonProperty("SpellDamage")]
        public long? SpellDamage { get; set; }

        [JsonProperty("MatchAwards")]
        public long[] MatchAwards { get; set; }
    }

    public partial class TeamExperience
    {
        [JsonProperty("TeamLevel")]
        public long? TeamLevel { get; set; }

        [JsonProperty("TimeSpan")]
        public DateTimeOffset TimeSpan { get; set; }

        [JsonProperty("MinionXP")]
        public long? MinionXp { get; set; }

        [JsonProperty("CreepXP")]
        public long? CreepXp { get; set; }

        [JsonProperty("StructureXP")]
        public long? StructureXp { get; set; }

        [JsonProperty("HeroXP")]
        public long? HeroXp { get; set; }

        [JsonProperty("TrickleXP")]
        public long? TrickleXp { get; set; }

        [JsonProperty("TotalXP")]
        public long? TotalXp { get; set; }
    }

    public partial class ReplayData
    {
        public static ReplayData FromJson(string json) => JsonConvert.DeserializeObject<ReplayData>(json, Converter.Settings);
    }
    public static class Serialize
    {
        public static string ToJson(this ReplayData self) => JsonConvert.SerializeObject(self, Converter.Settings);
    }

    internal static class Converter
    {
        public static readonly JsonSerializerSettings Settings = new JsonSerializerSettings
        {
            MetadataPropertyHandling = MetadataPropertyHandling.Ignore,
            DateParseHandling = DateParseHandling.None,
            Converters =
            {
                new IsoDateTimeConverter { DateTimeStyles = DateTimeStyles.AssumeUniversal }
            },
        };
    }
}