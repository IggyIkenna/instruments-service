"""
Team Normalizer — maps team name variants to canonical identifiers.

Data sources use inconsistent team names (abbreviations, nicknames, city names).
This module provides a single normalize() call that resolves any known variant
to a ``(canonical_id, display_name)`` tuple, or returns ``None`` for unrecognised names.

Supports:
- Static alias table for exact matches (fast path)
- Unicode / accent normalization (Alavés -> Alaves, München -> Munchen)
- Suffix stripping (FC, AFC, United, City, etc.)
- Historical team name changes
- Cross-provider aliases (Betfair, API-Football, Odds API, Understat)
"""

from __future__ import annotations

import logging
import re
import unicodedata

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Alias table — maps lowercase variant -> (canonical_id, display_name)
# ---------------------------------------------------------------------------

_TEAM_ALIASES: dict[str, tuple[str, str]] = {
    # ==================================================================
    # EPL -- current & recently promoted/relegated teams (2019-2026)
    # ==================================================================
    "arsenal": ("ARS", "Arsenal"),
    "arsenal fc": ("ARS", "Arsenal"),
    "afc": ("ARS", "Arsenal"),
    "aston villa": ("AVL", "Aston Villa"),
    "villa": ("AVL", "Aston Villa"),
    "bournemouth": ("BOU", "Bournemouth"),
    "afc bournemouth": ("BOU", "Bournemouth"),
    "brentford": ("BRE", "Brentford"),
    "brentford fc": ("BRE", "Brentford"),
    "brighton": ("BHA", "Brighton"),
    "brighton & hove albion": ("BHA", "Brighton"),
    "brighton and hove albion": ("BHA", "Brighton"),
    "burnley": ("BUR", "Burnley"),
    "burnley fc": ("BUR", "Burnley"),
    "cardiff": ("CAR", "Cardiff City"),
    "cardiff city": ("CAR", "Cardiff City"),
    "chelsea": ("CHE", "Chelsea"),
    "chelsea fc": ("CHE", "Chelsea"),
    "cfc": ("CHE", "Chelsea"),
    "crystal palace": ("CRY", "Crystal Palace"),
    "c palace": ("CRY", "Crystal Palace"),
    "palace": ("CRY", "Crystal Palace"),
    "everton": ("EVE", "Everton"),
    "everton fc": ("EVE", "Everton"),
    "fulham": ("FUL", "Fulham"),
    "fulham fc": ("FUL", "Fulham"),
    "huddersfield": ("HUD", "Huddersfield Town"),
    "huddersfield town": ("HUD", "Huddersfield Town"),
    "ipswich": ("IPS", "Ipswich Town"),
    "ipswich town": ("IPS", "Ipswich Town"),
    "leeds": ("LEE", "Leeds United"),
    "leeds united": ("LEE", "Leeds United"),
    "leeds utd": ("LEE", "Leeds United"),
    "leicester": ("LEI", "Leicester City"),
    "leicester city": ("LEI", "Leicester City"),
    "liverpool": ("LIV", "Liverpool"),
    "liverpool fc": ("LIV", "Liverpool"),
    "lfc": ("LIV", "Liverpool"),
    "luton": ("LUT", "Luton Town"),
    "luton town": ("LUT", "Luton Town"),
    "manchester city": ("MCI", "Manchester City"),
    "man city": ("MCI", "Manchester City"),
    "man c": ("MCI", "Manchester City"),
    "mcfc": ("MCI", "Manchester City"),
    "manchester united": ("MUN", "Manchester United"),
    "man utd": ("MUN", "Manchester United"),
    "man united": ("MUN", "Manchester United"),
    "manchester utd": ("MUN", "Manchester United"),
    "mufc": ("MUN", "Manchester United"),
    "newcastle": ("NEW", "Newcastle United"),
    "newcastle united": ("NEW", "Newcastle United"),
    "newcastle utd": ("NEW", "Newcastle United"),
    "norwich": ("NOR", "Norwich City"),
    "norwich city": ("NOR", "Norwich City"),
    "nottingham forest": ("NFO", "Nottingham Forest"),
    "nott'm forest": ("NFO", "Nottingham Forest"),
    "nottm forest": ("NFO", "Nottingham Forest"),
    "forest": ("NFO", "Nottingham Forest"),
    "sheffield united": ("SHU", "Sheffield United"),
    "sheff utd": ("SHU", "Sheffield United"),
    "sheff united": ("SHU", "Sheffield United"),
    "sheffield utd": ("SHU", "Sheffield United"),
    "sheffield u": ("SHU", "Sheffield United"),
    "southampton": ("SOU", "Southampton"),
    "southampton fc": ("SOU", "Southampton"),
    "sunderland": ("SUN", "Sunderland"),
    "sunderland afc": ("SUN", "Sunderland"),
    "tottenham": ("TOT", "Tottenham Hotspur"),
    "tottenham hotspur": ("TOT", "Tottenham Hotspur"),
    "spurs": ("TOT", "Tottenham Hotspur"),
    "thfc": ("TOT", "Tottenham Hotspur"),
    "watford": ("WAT", "Watford"),
    "watford fc": ("WAT", "Watford"),
    "west ham": ("WHU", "West Ham United"),
    "west ham united": ("WHU", "West Ham United"),
    "west ham utd": ("WHU", "West Ham United"),
    "wolves": ("WOL", "Wolverhampton Wanderers"),
    "wolverhampton": ("WOL", "Wolverhampton Wanderers"),
    "wolverhampton wanderers": ("WOL", "Wolverhampton Wanderers"),
    # Historical EPL teams (2010-2019)
    "birmingham": ("BIR", "Birmingham City"),
    "west brom": ("WBA", "West Brom"),
    "west bromwich albion": ("WBA", "West Brom"),
    "wigan": ("WIG", "Wigan Athletic"),
    "blackburn": ("BLB", "Blackburn Rovers"),
    "bolton": ("BOL", "Bolton Wanderers"),
    "stoke city": ("STK", "Stoke City"),
    "stoke": ("STK", "Stoke City"),
    "blackpool": ("BLP", "Blackpool"),
    "swansea": ("SWA", "Swansea City"),
    "reading": ("REA", "Reading"),
    "hull city": ("HUL", "Hull City"),
    "hull": ("HUL", "Hull City"),
    "middlesbrough": ("MID", "Middlesbrough"),
    "queens park rangers": ("QPR", "Queens Park Rangers"),
    "qpr": ("QPR", "Queens Park Rangers"),
    # ==================================================================
    # BUNDESLIGA — current & recently promoted/relegated teams
    # ==================================================================
    "arminia bielefeld": ("ARM", "Arminia Bielefeld"),
    "bielefeld": ("ARM", "Arminia Bielefeld"),
    "arminia": ("ARM", "Arminia Bielefeld"),
    "augsburg": ("AUG", "Augsburg"),
    "fc augsburg": ("AUG", "Augsburg"),
    "bayern munich": ("BAY", "Bayern Munich"),
    "bayern": ("BAY", "Bayern Munich"),
    "fc bayern munich": ("BAY", "Bayern Munich"),
    "bayern munchen": ("BAY", "Bayern Munich"),
    "bayern münchen": ("BAY", "Bayern Munich"),
    "bochum": ("BOC", "VfL Bochum"),
    "vfl bochum": ("BOC", "VfL Bochum"),
    "borussia dortmund": ("BVB", "Borussia Dortmund"),
    "dortmund": ("BVB", "Borussia Dortmund"),
    "bvb": ("BVB", "Borussia Dortmund"),
    "borussia monchengladbach": ("BMG", "Borussia Monchengladbach"),
    "borussia m'gladbach": ("BMG", "Borussia Monchengladbach"),
    "mgladbach": ("BMG", "Borussia Monchengladbach"),
    "monchengladbach": ("BMG", "Borussia Monchengladbach"),
    "m'gladbach": ("BMG", "Borussia Monchengladbach"),
    "borussia m.gladbach": ("BMG", "Borussia Monchengladbach"),
    "borussia mönchengladbach": ("BMG", "Borussia Monchengladbach"),
    "cologne": ("KOE", "FC Koln"),
    "fc koln": ("KOE", "FC Koln"),
    "fc cologne": ("KOE", "FC Koln"),
    "koln": ("KOE", "FC Koln"),
    "1.fc köln": ("KOE", "FC Koln"),
    "1. fc köln": ("KOE", "FC Koln"),
    "fc köln": ("KOE", "FC Koln"),
    "darmstadt": ("DAR", "SV Darmstadt 98"),
    "darmstadt 98": ("DAR", "SV Darmstadt 98"),
    "sv darmstadt 98": ("DAR", "SV Darmstadt 98"),
    "eintracht frankfurt": ("SGE", "Eintracht Frankfurt"),
    "frankfurt": ("SGE", "Eintracht Frankfurt"),
    "freiburg": ("SCF", "SC Freiburg"),
    "sc freiburg": ("SCF", "SC Freiburg"),
    "greuther furth": ("GRF", "Greuther Furth"),
    "greuther fürth": ("GRF", "Greuther Furth"),
    "spvgg greuther fürth": ("GRF", "Greuther Furth"),
    "spvgg greuther furth": ("GRF", "Greuther Furth"),
    "hamburger sv": ("HSV", "Hamburger SV"),
    "hamburg": ("HSV", "Hamburger SV"),
    "hsv": ("HSV", "Hamburger SV"),
    "hannover": ("H96", "Hannover 96"),
    "hannover 96": ("H96", "Hannover 96"),
    "hertha berlin": ("BSC", "Hertha Berlin"),
    "hertha": ("BSC", "Hertha Berlin"),
    "hertha bsc": ("BSC", "Hertha Berlin"),
    "hoffenheim": ("TSG", "TSG Hoffenheim"),
    "tsg hoffenheim": ("TSG", "TSG Hoffenheim"),
    "1899 hoffenheim": ("TSG", "TSG Hoffenheim"),
    "heidenheim": ("HDH", "FC Heidenheim"),
    "fc heidenheim": ("HDH", "FC Heidenheim"),
    "1. fc heidenheim": ("HDH", "FC Heidenheim"),
    "holstein kiel": ("KIE", "Holstein Kiel"),
    "kiel": ("KIE", "Holstein Kiel"),
    "kaiserslautern": ("FCK", "FC Kaiserslautern"),
    "fc kaiserslautern": ("FCK", "FC Kaiserslautern"),
    "1. fc kaiserslautern": ("FCK", "FC Kaiserslautern"),
    "karlsruhe": ("KSC", "Karlsruher SC"),
    "karlsruher sc": ("KSC", "Karlsruher SC"),
    "leverkusen": ("B04", "Bayer Leverkusen"),
    "bayer leverkusen": ("B04", "Bayer Leverkusen"),
    "mainz": ("M05", "Mainz 05"),
    "mainz 05": ("M05", "Mainz 05"),
    "fsv mainz 05": ("M05", "Mainz 05"),
    "paderborn": ("PAD", "SC Paderborn"),
    "sc paderborn": ("PAD", "SC Paderborn"),
    "sc paderborn 07": ("PAD", "SC Paderborn"),
    "rb leipzig": ("RBL", "RB Leipzig"),
    "leipzig": ("RBL", "RB Leipzig"),
    "rasenballsport leipzig": ("RBL", "RB Leipzig"),
    "schalke": ("S04", "Schalke 04"),
    "schalke 04": ("S04", "Schalke 04"),
    "fc schalke 04": ("S04", "Schalke 04"),
    "st pauli": ("STP", "FC St. Pauli"),
    "st. pauli": ("STP", "FC St. Pauli"),
    "fc st pauli": ("STP", "FC St. Pauli"),
    "fc st. pauli": ("STP", "FC St. Pauli"),
    "stuttgart": ("VFB", "VfB Stuttgart"),
    "vfb stuttgart": ("VFB", "VfB Stuttgart"),
    "union berlin": ("FCU", "Union Berlin"),
    "union": ("FCU", "Union Berlin"),
    "fc union berlin": ("FCU", "Union Berlin"),
    "werder bremen": ("SVW", "Werder Bremen"),
    "bremen": ("SVW", "Werder Bremen"),
    "sv werder bremen": ("SVW", "Werder Bremen"),
    "wolfsburg": ("WOB", "VfL Wolfsburg"),
    "vfl wolfsburg": ("WOB", "VfL Wolfsburg"),
    # Historical Bundesliga
    "1. fc nürnberg": ("FCN", "FC Nurnberg"),
    "fc nurnberg": ("FCN", "FC Nurnberg"),
    "nurnberg": ("FCN", "FC Nurnberg"),
    "nuernberg": ("FCN", "FC Nurnberg"),
    "fortuna dusseldorf": ("F95", "Fortuna Dusseldorf"),
    "fortuna düsseldorf": ("F95", "Fortuna Dusseldorf"),
    "fortuna duesseldorf": ("F95", "Fortuna Dusseldorf"),
    "eintracht braunschweig": ("EBS", "Eintracht Braunschweig"),
    "braunschweig": ("EBS", "Eintracht Braunschweig"),
    "fc ingolstadt 04": ("ING", "FC Ingolstadt"),
    "ingolstadt": ("ING", "FC Ingolstadt"),
    "sv elversberg": ("ELV", "SV Elversberg"),
    "elversberg": ("ELV", "SV Elversberg"),
    # ==================================================================
    # LA LIGA — top Spanish teams and common aliases
    # ==================================================================
    "real madrid": ("RMA", "Real Madrid"),
    "madrid": ("RMA", "Real Madrid"),
    "barcelona": ("BAR", "Barcelona"),
    "fc barcelona": ("BAR", "Barcelona"),
    "barca": ("BAR", "Barcelona"),
    "atletico madrid": ("ATM", "Atletico Madrid"),
    "atletico": ("ATM", "Atletico Madrid"),
    "atlético madrid": ("ATM", "Atletico Madrid"),
    "atlético de madrid": ("ATM", "Atletico Madrid"),
    "sevilla": ("SEV", "Sevilla"),
    "sevilla fc": ("SEV", "Sevilla"),
    "real sociedad": ("RSO", "Real Sociedad"),
    "sociedad": ("RSO", "Real Sociedad"),
    "villarreal": ("VIL", "Villarreal"),
    "villarreal cf": ("VIL", "Villarreal"),
    "real betis": ("BET", "Real Betis"),
    "betis": ("BET", "Real Betis"),
    "athletic bilbao": ("ATH", "Athletic Bilbao"),
    "athletic club": ("ATH", "Athletic Bilbao"),
    "bilbao": ("ATH", "Athletic Bilbao"),
    "valencia": ("VAL", "Valencia"),
    "valencia cf": ("VAL", "Valencia"),
    "getafe": ("GET", "Getafe"),
    "getafe cf": ("GET", "Getafe"),
    "celta vigo": ("CEL", "Celta Vigo"),
    "celta": ("CEL", "Celta Vigo"),
    "osasuna": ("OSA", "Osasuna"),
    "ca osasuna": ("OSA", "Osasuna"),
    "alaves": ("ALA", "Alaves"),
    "alavés": ("ALA", "Alaves"),
    "deportivo alaves": ("ALA", "Alaves"),
    "mallorca": ("MLL", "Mallorca"),
    "rcd mallorca": ("MLL", "Mallorca"),
    "cadiz": ("CAD", "Cadiz"),
    "cádiz": ("CAD", "Cadiz"),
    "cadiz cf": ("CAD", "Cadiz"),
    "espanyol": ("ESP", "Espanyol"),
    "rcd espanyol": ("ESP", "Espanyol"),
    "girona": ("GIR", "Girona"),
    "girona fc": ("GIR", "Girona"),
    "rayo vallecano": ("RAY", "Rayo Vallecano"),
    "rayo": ("RAY", "Rayo Vallecano"),
    "almeria": ("ALM", "Almeria"),
    "almería": ("ALM", "Almeria"),
    "ud almeria": ("ALM", "Almeria"),
    "las palmas": ("LPA", "Las Palmas"),
    "ud las palmas": ("LPA", "Las Palmas"),
    "huesca": ("HUE", "Huesca"),
    "sd huesca": ("HUE", "Huesca"),
    "leganes": ("LEG", "Leganes"),
    "leganés": ("LEG", "Leganes"),
    "cd leganes": ("LEG", "Leganes"),
    "real valladolid": ("VLL", "Real Valladolid"),
    "valladolid": ("VLL", "Real Valladolid"),
    # ==================================================================
    # SERIE A — top Italian teams
    # ==================================================================
    "juventus": ("JUV", "Juventus"),
    "inter milan": ("INT", "Inter Milan"),
    "inter": ("INT", "Inter Milan"),
    "internazionale": ("INT", "Inter Milan"),
    "ac milan": ("MIL", "AC Milan"),
    "milan": ("MIL", "AC Milan"),
    "napoli": ("NAP", "Napoli"),
    "ssc napoli": ("NAP", "Napoli"),
    "roma": ("ROM", "AS Roma"),
    "as roma": ("ROM", "AS Roma"),
    "lazio": ("LAZ", "Lazio"),
    "ss lazio": ("LAZ", "Lazio"),
    "atalanta": ("ATA", "Atalanta"),
    "fiorentina": ("FIO", "Fiorentina"),
    "acf fiorentina": ("FIO", "Fiorentina"),
    "torino": ("TOR_IT", "Torino"),
    "torino fc": ("TOR_IT", "Torino"),
    "bologna": ("BOL_IT", "Bologna"),
    "bologna fc": ("BOL_IT", "Bologna"),
    "sampdoria": ("SAM", "Sampdoria"),
    "sassuolo": ("SAS", "Sassuolo"),
    "udinese": ("UDI", "Udinese"),
    "genoa": ("GEN", "Genoa"),
    "genoa cfc": ("GEN", "Genoa"),
    "cagliari": ("CAG", "Cagliari"),
    "lecce": ("LEC", "Lecce"),
    "us lecce": ("LEC", "Lecce"),
    "empoli": ("EMP", "Empoli"),
    "hellas verona": ("VER", "Hellas Verona"),
    "verona": ("VER", "Hellas Verona"),
    "monza": ("MON", "Monza"),
    "ac monza": ("MON", "Monza"),
    "salernitana": ("SAL", "Salernitana"),
    "frosinone": ("FRO", "Frosinone"),
    "parma": ("PRM", "Parma"),
    "parma calcio 1913": ("PRM", "Parma"),
    "como": ("CMO", "Como"),
    "como 1907": ("CMO", "Como"),
    "venezia": ("VEN", "Venezia"),
    "venezia fc": ("VEN", "Venezia"),
    "spezia": ("SPE", "Spezia"),
    "cremonese": ("CRE", "Cremonese"),
    "spal": ("SPA", "SPAL"),
    "spal 2013": ("SPA", "SPAL"),
    # ==================================================================
    # LIGUE 1 — top French teams
    # ==================================================================
    "paris saint-germain": ("PSG", "Paris Saint-Germain"),
    "paris saint germain": ("PSG", "Paris Saint-Germain"),
    "psg": ("PSG", "Paris Saint-Germain"),
    "marseille": ("OM", "Marseille"),
    "olympique marseille": ("OM", "Marseille"),
    "olympique de marseille": ("OM", "Marseille"),
    "lyon": ("OL", "Lyon"),
    "olympique lyonnais": ("OL", "Lyon"),
    "olympique lyon": ("OL", "Lyon"),
    "monaco": ("MON_FR", "Monaco"),
    "as monaco": ("MON_FR", "Monaco"),
    "lille": ("LIL", "Lille"),
    "lille osc": ("LIL", "Lille"),
    "losc lille": ("LIL", "Lille"),
    "nice": ("NCE", "Nice"),
    "ogc nice": ("NCE", "Nice"),
    "rennes": ("REN", "Rennes"),
    "stade rennais": ("REN", "Rennes"),
    "lens": ("LEN", "Lens"),
    "rc lens": ("LEN", "Lens"),
    "strasbourg": ("STR", "Strasbourg"),
    "rc strasbourg": ("STR", "Strasbourg"),
    "nantes": ("NAN", "Nantes"),
    "fc nantes": ("NAN", "Nantes"),
    "montpellier": ("MTP", "Montpellier"),
    "montpellier hsc": ("MTP", "Montpellier"),
    "toulouse": ("TLS", "Toulouse"),
    "toulouse fc": ("TLS", "Toulouse"),
    "brest": ("BRS", "Brest"),
    "stade brestois 29": ("BRS", "Brest"),
    "stade brestois": ("BRS", "Brest"),
    "reims": ("REI", "Reims"),
    "stade de reims": ("REI", "Reims"),
    "clermont": ("CLF", "Clermont Foot"),
    "clermont foot": ("CLF", "Clermont Foot"),
    "le havre": ("LHV", "Le Havre"),
    "le havre ac": ("LHV", "Le Havre"),
    "metz": ("MTZ", "Metz"),
    "fc metz": ("MTZ", "Metz"),
    "lorient": ("LOR", "Lorient"),
    "fc lorient": ("LOR", "Lorient"),
    "saint-etienne": ("STE", "Saint-Etienne"),
    "saint-étienne": ("STE", "Saint-Etienne"),
    "as saint-etienne": ("STE", "Saint-Etienne"),
    "angers": ("ANG", "Angers"),
    "angers sco": ("ANG", "Angers"),
    "auxerre": ("AUX", "Auxerre"),
    "aj auxerre": ("AUX", "Auxerre"),
    # ==================================================================
    # EREDIVISIE — top Dutch teams
    # ==================================================================
    "ajax": ("AJX", "Ajax"),
    "afc ajax": ("AJX", "Ajax"),
    "psv": ("PSV_NL", "PSV Eindhoven"),
    "psv eindhoven": ("PSV_NL", "PSV Eindhoven"),
    "feyenoord": ("FEY", "Feyenoord"),
    "az alkmaar": ("AZ", "AZ Alkmaar"),
    "az": ("AZ", "AZ Alkmaar"),
    "twente": ("TWE", "FC Twente"),
    "fc twente": ("TWE", "FC Twente"),
    "fc utrecht": ("UTR", "FC Utrecht"),
    "utrecht": ("UTR", "FC Utrecht"),
    # ==================================================================
    # PRIMEIRA LIGA — top Portuguese teams
    # ==================================================================
    "benfica": ("BEN", "Benfica"),
    "sl benfica": ("BEN", "Benfica"),
    "porto": ("POR", "FC Porto"),
    "fc porto": ("POR", "FC Porto"),
    "sporting cp": ("SCP", "Sporting CP"),
    "sporting": ("SCP", "Sporting CP"),
    "sporting lisbon": ("SCP", "Sporting CP"),
    "braga": ("BRA_PT", "SC Braga"),
    "sc braga": ("BRA_PT", "SC Braga"),
    # ==================================================================
    # NBA
    # ==================================================================
    "lakers": ("LAL", "Los Angeles Lakers"),
    "los angeles lakers": ("LAL", "Los Angeles Lakers"),
    "la lakers": ("LAL", "Los Angeles Lakers"),
    "celtics": ("BOS", "Boston Celtics"),
    "boston celtics": ("BOS", "Boston Celtics"),
    "warriors": ("GSW", "Golden State Warriors"),
    "golden state warriors": ("GSW", "Golden State Warriors"),
    "golden state": ("GSW", "Golden State Warriors"),
    "bucks": ("MIL_NBA", "Milwaukee Bucks"),
    "milwaukee bucks": ("MIL_NBA", "Milwaukee Bucks"),
    # ==================================================================
    # NFL
    # ==================================================================
    "chiefs": ("KC", "Kansas City Chiefs"),
    "kansas city chiefs": ("KC", "Kansas City Chiefs"),
    "eagles": ("PHI", "Philadelphia Eagles"),
    "philadelphia eagles": ("PHI", "Philadelphia Eagles"),
    "49ers": ("SF", "San Francisco 49ers"),
    "san francisco 49ers": ("SF", "San Francisco 49ers"),
    "cowboys": ("DAL", "Dallas Cowboys"),
    "dallas cowboys": ("DAL", "Dallas Cowboys"),
    # ==================================================================
    # MLB
    # ==================================================================
    "yankees": ("NYY", "New York Yankees"),
    "new york yankees": ("NYY", "New York Yankees"),
    "dodgers": ("LAD", "Los Angeles Dodgers"),
    "los angeles dodgers": ("LAD", "Los Angeles Dodgers"),
    "red sox": ("BOS_MLB", "Boston Red Sox"),
    "boston red sox": ("BOS_MLB", "Boston Red Sox"),
    # ==================================================================
    # NHL
    # ==================================================================
    "maple leafs": ("TOR_NHL", "Toronto Maple Leafs"),
    "toronto maple leafs": ("TOR_NHL", "Toronto Maple Leafs"),
    "bruins": ("BOS_NHL", "Boston Bruins"),
    "boston bruins": ("BOS_NHL", "Boston Bruins"),
    "oilers": ("EDM", "Edmonton Oilers"),
    "edmonton oilers": ("EDM", "Edmonton Oilers"),
}


# ---------------------------------------------------------------------------
# Historical team name changes — API-Football team_id -> current name
# Used for enrichment; the dict maps (old_name_lower) -> canonical entry.
# ---------------------------------------------------------------------------

_HISTORICAL_NAME_CHANGES: dict[str, tuple[str, str]] = {
    # Sourced from team_name_changes.py
    "le havre": ("LHV", "Le Havre"),
    "fortuna dusseldorf": ("F95", "Fortuna Dusseldorf"),
    "fortuna düsseldorf": ("F95", "Fortuna Dusseldorf"),
    "borussia monchengladbach": ("BMG", "Borussia Monchengladbach"),
    "borussia mönchengladbach": ("BMG", "Borussia Monchengladbach"),
    "fc nurnberg": ("FCN", "FC Nurnberg"),
    "1. fc nürnberg": ("FCN", "FC Nurnberg"),
    "vfl bochum": ("BOC", "VfL Bochum"),
    "fc heidenheim": ("HDH", "FC Heidenheim"),
    "fc koln": ("KOE", "FC Koln"),
    "fc kaiserslautern": ("FCK", "FC Kaiserslautern"),
    "vfl osnabruck": ("OSN", "VfL Osnabruck"),
    "vfl osnabrück": ("OSN", "VfL Osnabruck"),
    "fc viktoria koln": ("VIK", "Viktoria Koln"),
    "rot-weiss essen": ("RWE", "Rot-Weiss Essen"),
    "fc saarbrucken": ("SAA", "FC Saarbrucken"),
    "fc saarbrücken": ("SAA", "FC Saarbrucken"),
    "ssv ulm 1846": ("ULM", "SSV Ulm 1846"),
    "preussen munster": ("PRM_DE", "Preussen Munster"),
    "preussen münster": ("PRM_DE", "Preussen Munster"),
    "olympiakos piraeus": ("OLY", "Olympiakos"),
    "olympiakos": ("OLY", "Olympiakos"),
    "como": ("CMO", "Como"),
    "calcio padova": ("PAD_IT", "Padova"),
}


# ---------------------------------------------------------------------------
# Suffix stripping — common suffixes to remove during fuzzy matching
# ---------------------------------------------------------------------------

_TEAM_SUFFIXES: tuple[str, ...] = (
    "fc",
    "afc",
    "cf",
    "sc",
    "ac",
    "united",
    "city",
    "town",
    "utd",
    "utd.",
    "f.c.",
    "a.f.c.",
    "s.c.",
    "c.f.",
    "a.c.",
    "football club",
    "athletic club",
    "soccer club",
    "fußball-club",
    "fußballclub",
    "voetbalclub",
)

# Pre-compiled pattern for accent removal
_ACCENT_RE = re.compile(r"[^a-z0-9\s\-]")
_MULTI_SPACE_RE = re.compile(r"\s+")


def _strip_accents(text: str) -> str:
    """Remove accents/diacritics from *text* and lowercase.

    ``Alavés`` -> ``alaves``, ``München`` -> ``munchen``.
    """
    nfd = unicodedata.normalize("NFD", text)
    stripped = "".join(ch for ch in nfd if unicodedata.category(ch) != "Mn")
    return stripped.lower()


def normalize_for_fuzzy(name: str) -> str:
    """Produce a simplified key for fuzzy comparison.

    1. Strip accents.
    2. Lowercase.
    3. Remove non-alphanumeric except spaces/hyphens.
    4. Collapse whitespace.
    """
    key = _strip_accents(name.strip())
    key = _ACCENT_RE.sub("", key)
    key = _MULTI_SPACE_RE.sub(" ", key)
    return key.strip()


def _extract_core_name(name: str) -> str:
    """Remove common club suffixes to get the distinctive core name."""
    normalized = normalize_for_fuzzy(name)
    for suffix in _TEAM_SUFFIXES:
        suffix_norm = normalize_for_fuzzy(suffix)
        if normalized.endswith(" " + suffix_norm):
            normalized = normalized[: -len(suffix_norm) - 1].strip()
        if normalized.startswith(suffix_norm + " "):
            normalized = normalized[len(suffix_norm) + 1 :].strip()
    return normalized


class TeamNormalizer:
    """Normalizes team name variants to canonical ``(id, display_name)`` pairs.

    The normalizer is stateless and uses a built-in alias table.
    Additional aliases can be registered at runtime via :meth:`register_alias`.

    Lookup strategy (in order):
    1. Exact match in alias table (case-insensitive, stripped).
    2. Accent-stripped match — ``München`` matches ``munchen``.
    3. Core-name match — stripping common suffixes (FC, United, etc.).
    4. Historical name change lookup.
    """

    def __init__(self) -> None:
        # Copy the module-level table so runtime additions are instance-scoped
        self._aliases: dict[str, tuple[str, str]] = dict(_TEAM_ALIASES)

        # Build accent-stripped reverse index for fuzzy matching
        self._accent_index: dict[str, tuple[str, str]] = {}
        for key, value in self._aliases.items():
            accent_key = normalize_for_fuzzy(key)
            if accent_key not in self._accent_index:
                self._accent_index[accent_key] = value

        # Build core-name reverse index
        self._core_index: dict[str, tuple[str, str]] = {}
        for key, value in self._aliases.items():
            core = _extract_core_name(key)
            if core and len(core) > 3 and core not in self._core_index:
                self._core_index[core] = value

        # Historical name changes
        self._historical: dict[str, tuple[str, str]] = dict(_HISTORICAL_NAME_CHANGES)

    def normalize(self, name: str) -> tuple[str, str] | None:
        """Return ``(canonical_id, display_name)`` for *name*, or ``None`` if unknown.

        Lookup is case-insensitive and strips leading/trailing whitespace.
        Tries exact match, then accent-stripped, then core-name, then historical.
        """
        key = name.strip().lower()

        # 1. Exact match
        result = self._aliases.get(key)
        if result is not None:
            return result

        # 2. Accent-stripped match
        accent_key = normalize_for_fuzzy(name)
        result = self._accent_index.get(accent_key)
        if result is not None:
            return result

        # 3. Core-name match (suffix stripping)
        core_key = _extract_core_name(name)
        if core_key and len(core_key) > 3:
            result = self._core_index.get(core_key)
            if result is not None:
                return result

        # 4. Historical name changes
        result = self._historical.get(key)
        if result is not None:
            return result
        result = self._historical.get(accent_key)
        if result is not None:
            return result

        logger.debug("Team name not found in alias table: %r", name)
        return None

    def register_alias(self, alias: str, canonical_id: str, display_name: str) -> None:
        """Add or overwrite an alias mapping at runtime."""
        key = alias.strip().lower()
        value = (canonical_id, display_name)
        self._aliases[key] = value
        self._accent_index[normalize_for_fuzzy(alias)] = value
        core = _extract_core_name(alias)
        if core and len(core) > 3:
            self._core_index[core] = value

    @property
    def known_team_count(self) -> int:
        """Number of unique canonical team IDs currently registered."""
        return len({v[0] for v in self._aliases.values()})
