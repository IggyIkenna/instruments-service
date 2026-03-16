"""
TradFi exchange mappings for instruments-service.

Databento parent/options symbol validation and exchange code mappings.
Extracted from venue_config.py per file-splitting-guide (codex compliance).
"""

# ============================================================================
# VALID DATABENTO PARENT SYMBOLS - Used for validation in market-tick-data-service
# ============================================================================
# Maps canonical underlying -> (databento_parent_symbol, dataset)
# Only symbols in this map are valid for parent symbology downloads
DATABENTO_VALID_PARENT_SYMBOLS: dict[str, tuple[str, str]] = {
    # CME Index Futures
    "ES": ("ES.FUT", "GLBX.MDP3"),
    "SP500": ("ES.FUT", "GLBX.MDP3"),
    "NQ": ("NQ.FUT", "GLBX.MDP3"),
    "NASDAQ100": ("NQ.FUT", "GLBX.MDP3"),
    "RTY": ("RTY.FUT", "GLBX.MDP3"),
    "RUSSELL2000": ("RTY.FUT", "GLBX.MDP3"),
    "YM": ("YM.FUT", "GLBX.MDP3"),
    "DOW": ("YM.FUT", "GLBX.MDP3"),
    # CME Energy
    "CL": ("CL.FUT", "GLBX.MDP3"),
    "CRUDE": ("CL.FUT", "GLBX.MDP3"),
    "NG": ("NG.FUT", "GLBX.MDP3"),
    "NATGAS": ("NG.FUT", "GLBX.MDP3"),
    "RB": ("RB.FUT", "GLBX.MDP3"),
    "GASOLINE": ("RB.FUT", "GLBX.MDP3"),
    "HO": ("HO.FUT", "GLBX.MDP3"),
    "HEATINGOIL": ("HO.FUT", "GLBX.MDP3"),
    # CME Metals
    "GC": ("GC.FUT", "GLBX.MDP3"),
    "GOLD": ("GC.FUT", "GLBX.MDP3"),
    "SI": ("SI.FUT", "GLBX.MDP3"),
    "SILVER": ("SI.FUT", "GLBX.MDP3"),
    "HG": ("HG.FUT", "GLBX.MDP3"),
    "COPPER": ("HG.FUT", "GLBX.MDP3"),
    "PL": ("PL.FUT", "GLBX.MDP3"),
    "PLATINUM": ("PL.FUT", "GLBX.MDP3"),
    # CME Grains
    "ZC": ("ZC.FUT", "GLBX.MDP3"),
    "CORN": ("ZC.FUT", "GLBX.MDP3"),
    "ZW": ("ZW.FUT", "GLBX.MDP3"),
    "WHEAT": ("ZW.FUT", "GLBX.MDP3"),
    "ZS": ("ZS.FUT", "GLBX.MDP3"),
    "SOYBEAN": ("ZS.FUT", "GLBX.MDP3"),
    "ZM": ("ZM.FUT", "GLBX.MDP3"),
    "SOYMEAL": ("ZM.FUT", "GLBX.MDP3"),
    "ZL": ("ZL.FUT", "GLBX.MDP3"),
    "SOYOIL": ("ZL.FUT", "GLBX.MDP3"),
    # CME Interest Rates
    "ZB": ("ZB.FUT", "GLBX.MDP3"),
    "TBOND": ("ZB.FUT", "GLBX.MDP3"),
    "ZN": ("ZN.FUT", "GLBX.MDP3"),
    "TNOTE10Y": ("ZN.FUT", "GLBX.MDP3"),
    "ZF": ("ZF.FUT", "GLBX.MDP3"),
    "TNOTE5Y": ("ZF.FUT", "GLBX.MDP3"),
    "ZT": ("ZT.FUT", "GLBX.MDP3"),
    "TNOTE2Y": ("ZT.FUT", "GLBX.MDP3"),
    # CME Currencies
    "6E": ("6E.FUT", "GLBX.MDP3"),
    "EUR": ("6E.FUT", "GLBX.MDP3"),
    "6J": ("6J.FUT", "GLBX.MDP3"),
    "JPY": ("6J.FUT", "GLBX.MDP3"),
    "6B": ("6B.FUT", "GLBX.MDP3"),
    "GBP": ("6B.FUT", "GLBX.MDP3"),
    "6C": ("6C.FUT", "GLBX.MDP3"),
    "CAD": ("6C.FUT", "GLBX.MDP3"),
    "6A": ("6A.FUT", "GLBX.MDP3"),
    "AUD": ("6A.FUT", "GLBX.MDP3"),
    "6S": ("6S.FUT", "GLBX.MDP3"),
    "CHF": ("6S.FUT", "GLBX.MDP3"),
    "6M": ("6M.FUT", "GLBX.MDP3"),
    "MXN": ("6M.FUT", "GLBX.MDP3"),
    "6N": ("6N.FUT", "GLBX.MDP3"),
    "NZD": ("6N.FUT", "GLBX.MDP3"),
    "6L": ("6L.FUT", "GLBX.MDP3"),
    "BRL": ("6L.FUT", "GLBX.MDP3"),
    "6Z": ("6Z.FUT", "GLBX.MDP3"),
    "ZAR": ("6Z.FUT", "GLBX.MDP3"),
    # CME Livestock
    "LE": ("LE.FUT", "GLBX.MDP3"),
    "LIVECATTLE": ("LE.FUT", "GLBX.MDP3"),
    "HE": ("HE.FUT", "GLBX.MDP3"),
    "LEANHOGS": ("HE.FUT", "GLBX.MDP3"),
    # CFE (CBOE Futures Exchange) - Volatility Futures
    "VX": ("VX.FUT", "XCBF.MDP3"),
    "VIX_FUT": ("VX.FUT", "XCBF.MDP3"),
    # ICE Futures US (IFUS.IMPACT) - Available from 2018-12-23
    "CT": ("CT.FUT", "IFUS.IMPACT"),
    "COTTON": ("CT.FUT", "IFUS.IMPACT"),
    "CC": ("CC.FUT", "IFUS.IMPACT"),
    "COCOA": ("CC.FUT", "IFUS.IMPACT"),
    "KC": ("KC.FUT", "IFUS.IMPACT"),
    "COFFEE": ("KC.FUT", "IFUS.IMPACT"),
    "SB": ("SB.FUT", "IFUS.IMPACT"),
    "SUGAR": ("SB.FUT", "IFUS.IMPACT"),
    "OJ": ("OJ.FUT", "IFUS.IMPACT"),
    "ORANGEJUICE": ("OJ.FUT", "IFUS.IMPACT"),
    "DX": ("DX.FUT", "IFUS.IMPACT"),
    "DOLLARINDEX": ("DX.FUT", "IFUS.IMPACT"),
    # ICE Futures Europe - Energy (available from Dec 23, 2018)
    "BRN": ("BRN.FUT", "IFEU.IMPACT"),
    "BRENT": ("BRN.FUT", "IFEU.IMPACT"),
    "G": ("G.FUT", "IFEU.IMPACT"),
    "GASOIL": ("G.FUT", "IFEU.IMPACT"),
    "T": ("T.FUT", "IFEU.IMPACT"),
    "WTI": ("T.FUT", "IFEU.IMPACT"),
}

# Options parent symbol mapping
DATABENTO_VALID_OPTIONS_SYMBOLS: dict[str, tuple[str, str]] = {
    "ES": ("ES.OPT", "GLBX.MDP3"),
    "SP500": ("ES.OPT", "GLBX.MDP3"),
    "NQ": ("NQ.OPT", "GLBX.MDP3"),
    "NASDAQ100": ("NQ.OPT", "GLBX.MDP3"),
    "CL": ("CL.OPT", "GLBX.MDP3"),
    "CRUDE": ("CL.OPT", "GLBX.MDP3"),
    "GC": ("GC.OPT", "GLBX.MDP3"),
    "GOLD": ("GC.OPT", "GLBX.MDP3"),
}

EXCHANGE_CODE_TO_NAME: dict[str, str] = {
    # CME Index
    "ES": "SP500",
    "NQ": "NASDAQ100",
    "RTY": "RUSSELL2000",
    "YM": "DOW",
    # CME Energy
    "CL": "CRUDE",
    "NG": "NATGAS",
    "RB": "GASOLINE",
    "HO": "HEATINGOIL",
    # CME Metals
    "GC": "GOLD",
    "SI": "SILVER",
    "HG": "COPPER",
    "PL": "PLATINUM",
    # CME Grains
    "ZC": "CORN",
    "ZW": "WHEAT",
    "ZS": "SOYBEAN",
    "ZM": "SOYMEAL",
    "ZL": "SOYOIL",
    # CME Interest Rates
    "ZB": "TBOND",
    "ZN": "TNOTE10Y",
    "ZF": "TNOTE5Y",
    "ZT": "TNOTE2Y",
    # CME Currencies
    "6E": "EUR",
    "6J": "JPY",
    "6B": "GBP",
    "6C": "CAD",
    "6A": "AUD",
    "6S": "CHF",
    "6L": "BRL",
    "6N": "NZD",
    "6Z": "ZAR",
    "6M": "MXN",
    # CME Livestock
    "LE": "LIVECATTLE",
    "HE": "LEANHOGS",
    # CFE Volatility
    "VX": "VIX",
    # CME Options (weekly)
    "EW1": "SP500",
    "EW2": "SP500",
    "EW3": "SP500",
    "EW4": "SP500",
    # ICE US
    "CT": "COTTON",
    "CC": "COCOA",
    "KC": "COFFEE",
    "SB": "SUGAR",
    "OJ": "ORANGEJUICE",
    "DX": "DOLLARINDEX",
    # ICE Europe
    "BRN": "BRENT",
    "G": "GASOIL",
    "T": "WTI",
}
