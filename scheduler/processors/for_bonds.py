from datetime import datetime, date
from typing import List, Dict, Any
import time


def process_bonds_data(raw_data: Dict) -> List[Dict[str, Any]]:
    """
    Преобразует сырые данные с MOEX API в список словарей для модели BondsMarketData.
    """
    start = time.time()

    # === 1. Обработка marketdata_yields (данные по ценам) ===
    yields_data = raw_data.get("marketdata_yields", {}).get("data", [])
    yields_columns = raw_data.get("marketdata_yields", {}).get("columns", [])
    yields_idx = {col: idx for idx, col in enumerate(yields_columns)}

    yields_dict = {}
    for row in yields_data:
        secid = row[yields_idx["SECID"]]
        boardid = row[yields_idx["BOARDID"]]
        key = (secid, boardid)

        yielddate_str = row[yields_idx["YIELDDATE"]]
        try:
            yielddate = datetime.strptime(yielddate_str, "%Y-%m-%d").date() if yielddate_str else None
        except:
            yielddate = None

        price_raw = row[yields_idx["PRICE"]]
        price = float(price_raw) if price_raw is not None else None

        duration_raw = row[yields_idx["DURATION"]]
        duration_days = int(duration_raw) if duration_raw is not None else None

        waprice_raw = row[yields_idx["WAPRICE"]]
        waprice = float(waprice_raw) if waprice_raw is not None else None

        yields_dict[key] = {
            "price": price,
            "duration_days": duration_days,
            "waprice": waprice,
            "yielddate": yielddate,
        }

    # === 2. Обработка marketdata (теперь берём YIELD как effectiveyield) ===
    market_data = raw_data.get("marketdata", {}).get("data", [])
    market_columns = raw_data.get("marketdata", {}).get("columns", [])
    market_idx = {col: idx for idx, col in enumerate(market_columns)}

    market_dict = {}
    for row in market_data:
        secid = row[market_idx["SECID"]]
        boardid = row[market_idx["BOARDID"]]
        key = (secid, boardid)

        yield_raw = row[market_idx["YIELD"]]
        yield_val = float(yield_raw) if yield_raw is not None else None

        market_dict[key] = {
            "valtoday": row[market_idx["VALTODAY"]],
            "numtrades": row[market_idx["NUMTRADES"]],
            "effectiveyield": yield_val,
            "waprice_md": row[market_idx["WAPRICE"]],
        }

    # === 3. Обработка securities (теперь с PREVPRICE и FACEUNIT) ===
    sec_data = raw_data.get("securities", {}).get("data", [])
    sec_columns = raw_data.get("securities", {}).get("columns", [])
    sec_idx = {col: idx for idx, col in enumerate(sec_columns)}

    sec_dict = {}
    for row in sec_data:
        secid = row[sec_idx["SECID"]]

        try:
            facevalue = float(row[sec_idx["FACEVALUE"]])
        except Exception:
            facevalue = None

        try:
            accruedint = float(row[sec_idx["ACCRUEDINT"]])
        except Exception:
            accruedint = None

        try:
            prevprice_raw = row[sec_idx["PREVPRICE"]]
            prevprice = float(prevprice_raw) if prevprice_raw is not None else None
        except Exception:
            prevprice = None

        sec_dict[secid] = {
            "isin": row[sec_idx["ISIN"]],
            "shortname": row[sec_idx["SHORTNAME"]],
            "listlevel": int(row[sec_idx["LISTLEVEL"]]) if row[sec_idx["LISTLEVEL"]] else None,
            "matdate_str": row[sec_idx["MATDATE"]],
            "couponpercent": float(row[sec_idx["COUPONPERCENT"]]) if row[sec_idx["COUPONPERCENT"]] else None,
            "couponvalue": float(row[sec_idx["COUPONVALUE"]]) if row[sec_idx["COUPONVALUE"]] else None,
            "couponperiod": int(row[sec_idx["COUPONPERIOD"]]) if row[sec_idx["COUPONPERIOD"]] else None,
            "nextcoupon_str": row[sec_idx["NEXTCOUPON"]],
            "facevalue": facevalue,
            "lotsize": int(row[sec_idx["LOTSIZE"]]) if row[sec_idx["LOTSIZE"]] else None,
            "currency": row[sec_idx["FACEUNIT"]],
            "issuesize": int(row[sec_idx["ISSUESIZE"]]) if row[sec_idx["ISSUESIZE"]] else None,
            "issuesizeplaced": int(row[sec_idx["ISSUESIZEPLACED"]]) if row[sec_idx["ISSUESIZEPLACED"]] else None,
            "accruedint": accruedint,
            "prevprice": prevprice,
        }

    # === 4. Формируем итоговый результат ===
    result = []

    for (secid, boardid), yield_data in yields_dict.items():
        sec_info = sec_dict.get(secid)
        if not sec_info:
            continue

        market_info = market_dict.get((secid, boardid), {})

        try:
            matdate = datetime.strptime(sec_info["matdate_str"], "%Y-%m-%d").date() if sec_info["matdate_str"] else None
        except Exception:
            matdate = None

        try:
            nextcoupon = datetime.strptime(sec_info["nextcoupon_str"], "%Y-%m-%d").date() if sec_info["nextcoupon_str"] else None
        except Exception:
            nextcoupon = None

        # === Основные цены ===
        current_price = yield_data["price"]  # ✅ Текущая цена
        prev_price = sec_info["prevprice"]   # ✅ Вчерашняя цена

        # Расчёт изменений
        if current_price is not None and prev_price is not None:
            lastchange = round(current_price - prev_price, 6)
            lastchangeprcnt = round((current_price - prev_price) / prev_price * 100, 4) if prev_price != 0 else None
        else:
            lastchange = None
            lastchangeprcnt = None

        # ✅ ПРАВИЛЬНЫЙ расчёт полной цены (с НКД)
        accruedint = sec_info["accruedint"]
        if current_price is not None and accruedint is not None:
            # Полная цена = текущая цена + НКД (в процентах от номинала)
            # Если current_price в процентах, а accruedint в денежных единицах,
            # нужно привести к одинаковой размерности
            full_price = round(current_price + (accruedint / sec_info["facevalue"] * 100), 4)
        else:
            full_price = None

        duration_days = yield_data.get("duration_days")
        duration_years = round(duration_days / 365.0, 4) if duration_days else None

        item = {
            # --- Ключи ---
            "secid": secid,
            "boardid": boardid,
            "instrument_type": "bond",  # ✅ Обязательно задай тип!

            # --- Идентификация ---
            "isin": sec_info["isin"],
            "shortname": sec_info["shortname"],

            # --- Статус ---
            "list_level": sec_info["listlevel"],  # ✅ исправлено

            # --- Характеристики ---
            "maturity_date": matdate,  # ✅ исправлено
            "couponpercent": sec_info["couponpercent"],
            "couponvalue": sec_info["couponvalue"],
            "couponperiod": sec_info["couponperiod"],
            "next_coupon_date": nextcoupon,  # ✅ исправлено
            "facevalue": sec_info["facevalue"],
            "lotsize": sec_info["lotsize"],
            "currency": sec_info["currency"],
            "issuesize": sec_info["issuesize"],
            "issuesizeplaced": sec_info["issuesizeplaced"],

            # --- Рыночные данные ---
            "last_price": current_price,  # ✅ исправлено
            "change_abs": lastchange,  # ✅ исправлено
            "change_percent": lastchangeprcnt,  # ✅ исправлено
            "effectiveyield": market_info.get("effectiveyield"),
            "duration_days": duration_days,
            "duration_years": duration_years,

            # --- Ликвидность ---
            "volume": market_info.get("valtoday"),  # ✅ исправлено
            "trades_count": market_info.get("numtrades"),  # ✅ исправлено

            # --- НКД и полная цена ---
            "accruedint": accruedint,
            "full_price": full_price,
        }

        result.append(item)

    print(f"Processing time: {time.time() - start:.3f}s")
    return result