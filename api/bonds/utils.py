from typing import List, Dict, Any, Optional

def parse_bond_payments(data: dict) -> List[Dict[str, Any]]:
    """
    Парсит ответ с bondization.json и возвращает список событий:
    купоны, оферты, амортизации, погашения.
    """
    payments: List[Dict[str, Any]] = []

    # === 1. Купоны ===
    if "coupons" in data and data["coupons"].get("data"):
        columns = data["coupons"]["columns"]
        for row in data["coupons"]["data"]:
            coupon = dict(zip(columns, row))
            payments.append({
                "event_type": "COUPON",
                "event_date": coupon["coupondate"],  # "YYYY-MM-DD"
                "face_value": float(coupon["facevalue"]) if coupon["facevalue"] is not None else None,
                "payment_amount": float(coupon["value"]) if coupon["value"] is not None else None,
                "payment_amount_rub": float(coupon["value_rub"]) if coupon["value_rub"] is not None else None,
                "payment_percent": float(coupon["valueprc"]) if coupon["valueprc"] is not None else None,
                "record_date": coupon["recorddate"],
                "start_date": coupon["startdate"],
                "currency": coupon["faceunit"],
                "source": "coupon"
            })

    # === 2. Оферты ===
    if "offers" in data and data["offers"].get("data"):
        columns = data["offers"]["columns"]
        for row in data["offers"]["data"]:
            offer = dict(zip(columns, row))
            payments.append({
                "event_type": "OFFER",
                "event_date": offer["offerdate"],
                "face_value": float(offer["facevalue"]) if offer["facevalue"] is not None else None,
                "payment_amount": float(offer["value"]) if offer["value"] is not None else None,
                "payment_amount_rub": float(offer["value"]) if offer["value"] is not None else None,  # value == value_rub в офертах?
                "offer_start_date": offer["offerdatestart"],
                "offer_end_date": offer["offerdateend"],
                "offer_price_percent": float(offer["price"]) if offer["price"] is not None else None,
                "offer_status": offer["offertype"],
                "currency": offer["faceunit"],
                "source": "offer"
            })

    # === 3. Амортизации / Погашение ===
    if "amortizations" in data and data["amortizations"].get("data"):
        columns = data["amortizations"]["columns"]
        for row in data["amortizations"]["data"]:
            amort = dict(zip(columns, row))
            # Определяем погашение по data_source = "maturity"
            is_maturity = (amort.get("data_source") or "").lower() == "maturity"
            event_type = "MATURITY" if is_maturity else "AMORTIZATION"

            payments.append({
                "event_type": event_type,
                "event_date": amort["amortdate"],
                "face_value": float(amort["facevalue"]) if amort["facevalue"] is not None else None,
                "payment_amount": float(amort["value"]) if amort["value"] is not None else None,
                "payment_amount_rub": float(amort["value_rub"]) if amort["value_rub"] is not None else None,
                "payment_percent": float(amort["valueprc"]) if amort["valueprc"] is not None else None,
                "currency": amort["faceunit"],
                "source": "amortization"
            })

    # === Сортировка по дате (строки в формате YYYY-MM-DD корректно сортируются лексикографически) ===
    payments.sort(key=lambda x: x["event_date"])

    return payments