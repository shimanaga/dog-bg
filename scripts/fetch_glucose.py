"""
LibreLinkUp → Firebase 血糖値データ取得スクリプト

GitHub Actionsから定期実行し、
LibreLinkUpのAPIからグルコースデータを取得してFirestoreに書き込む。

必要な環境変数:
  LIBRELINK_EMAIL       - LibreLinkUpのログインメールアドレス
  LIBRELINK_PASSWORD    - LibreLinkUpのパスワード
  FIREBASE_SERVICE_ACCOUNT - Firebaseサービスアカウントキー(JSON文字列)

任意:
  LIBRELINK_API_URL     - 例: https://api-jp.libreview.io （未指定ならこれ）
  LIBRELINK_PATIENT_ID  - 患者(接続)IDを明示指定（複数いる場合に推奨）
  LIBRELINK_PATIENT_INDEX - 0,1,2...（PATIENT_ID未指定時の選択）
  LIBRELINK_ASSUME_TZ   - timestampがtz無しの場合に仮定するTZ（例: Asia/Tokyo, UTC）
  DEBUG_INTERVALS       - 1 にすると取得間隔(分)の統計を出す
"""

import os
import sys
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import firebase_admin
from firebase_admin import credentials, firestore
from pylibrelinkup import PyLibreLinkUp

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


def get_env(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        print(f"ERROR: 環境変数 {name} が設定されていません")
        sys.exit(1)
    return val


def get_env_opt(name: str, default: str = "") -> str:
    v = os.environ.get(name, "")
    v = v.strip() if isinstance(v, str) else ""
    return v if v else default


def init_firebase() -> firestore.Client:
    sa_json = get_env("FIREBASE_SERVICE_ACCOUNT")
    sa_dict = json.loads(sa_json)
    cred = credentials.Certificate(sa_dict)
    # Actionsで複数回呼ばれても落ちないように
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    return firestore.client()


def _extract_patient_id(patient: Any) -> Optional[str]:
    """
    pylibrelinkup の Patient 表現がバージョンで揺れるので、
    ありがちな属性名/キーを総当りで拾う。
    """
    if patient is None:
        return None

    # dictの場合
    if isinstance(patient, dict):
        for k in ("patient_id", "patientId", "id", "connectionId", "connection_id"):
            v = patient.get(k)
            if v:
                return str(v)

    # objectの場合
    for attr in ("patient_id", "patientId", "id", "connectionId", "connection_id"):
        if hasattr(patient, attr):
            v = getattr(patient, attr)
            if v:
                return str(v)

    # 最後の手段: 文字列からUUIDっぽいものを抜く（苦肉）
    s = str(patient)
    m = re.search(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", s)
    if m:
        return m.group(0)

    return None


def _pick_patient(api: PyLibreLinkUp) -> Tuple[Any, str]:
    patients = api.get_patients()
    if not patients:
        raise RuntimeError("患者（ペット）が見つかりません")

    forced_id = get_env_opt("LIBRELINK_PATIENT_ID")
    if forced_id:
        # forced_id を優先。patients 内にいなくてもAPI的には通る場合があるのでそのまま使う
        return patients[0], forced_id

    idx_s = get_env_opt("LIBRELINK_PATIENT_INDEX", "0")
    try:
        idx = int(idx_s)
    except ValueError:
        idx = 0
    idx = max(0, min(idx, len(patients) - 1))
    patient = patients[idx]

    pid = _extract_patient_id(patient)
    if not pid:
        raise RuntimeError(f"patient_id を特定できません: {patient!r}")

    return patient, pid


def _dt_to_epoch_ms(dt: Any, assume_tz_name: str) -> int:
    """
    Libre側の timestamp が datetime の想定。
    tz無し(datetime naive)だった場合は assume_tz を付与してからUTCに変換。
    """
    if isinstance(dt, (int, float)):
        # 秒 or ミリ秒の可能性
        v = int(dt)
        return v if v > 10**12 else v * 1000

    if isinstance(dt, str):
        # ISO文字列の可能性（保険）
        try:
            parsed = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        except Exception:
            parsed = datetime.fromtimestamp(float(dt), tz=timezone.utc)
        dt = parsed

    if not isinstance(dt, datetime):
        raise TypeError(f"Unsupported timestamp type: {type(dt)}")

    if dt.tzinfo is None:
        if ZoneInfo is None:
            # zoneinfoが無い環境ならUTC扱いで進める
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            try:
                tz = ZoneInfo(assume_tz_name)
            except Exception:
                tz = timezone.utc
            dt = dt.replace(tzinfo=tz)

    return int(dt.astimezone(timezone.utc).timestamp() * 1000)


def _norm_value(meas: Any) -> Tuple[int, bool]:
    # value / isHi の取り方もバージョンで揺れるので吸収
    if isinstance(meas, dict):
        v = meas.get("value")
        is_hi = bool(meas.get("isHi") or meas.get("is_hi"))
    else:
        v = getattr(meas, "value", None)
        is_hi = bool(getattr(meas, "isHi", False) or getattr(meas, "is_hi", False))

    if isinstance(v, str):
        if v.strip().upper() == "HI":
            return 500, True
        try:
            v_num = int(float(v))
        except Exception:
            v_num = 0
        return v_num, is_hi

    try:
        v_num = int(v)
    except Exception:
        v_num = 0

    if v_num >= 500:
        is_hi = True
        v_num = 500

    return v_num, is_hi


def _get_timestamp(meas: Any) -> Any:
    if isinstance(meas, dict):
        return meas.get("timestamp") or meas.get("time") or meas.get("date")
    return getattr(meas, "timestamp", None)


def _fetch_graph(api: PyLibreLinkUp, patient_id: str) -> List[Any]:
    # pylibrelinkup のメソッド名揺れ吸収
    if hasattr(api, "get_graph_data"):
        return api.get_graph_data(patient_id)
    if hasattr(api, "graph"):
        return api.graph(patient_id)
    raise RuntimeError("graph取得メソッドが見つかりません（pylibrelinkupのバージョン差異）")


def _fetch_logbook(api: PyLibreLinkUp, patient_id: str) -> List[Any]:
    if hasattr(api, "get_logbook"):
        return api.get_logbook(patient_id)
    if hasattr(api, "logbook"):
        return api.logbook(patient_id)
    # logbook無くても動くように
    return []


def _debug_intervals(ms_list: List[Dict[str, Any]]) -> None:
    ts = [m["timestamp"] for m in ms_list]
    if len(ts) < 2:
        print("DEBUG: interval stats: not enough points")
        return
    ts.sort()
    deltas_min: List[int] = []
    for i in range(1, len(ts)):
        deltas_min.append(int(round((ts[i] - ts[i - 1]) / 60000.0)))
    # 頻度上位
    freq: Dict[int, int] = {}
    for d in deltas_min:
        freq[d] = freq.get(d, 0) + 1
    top = sorted(freq.items(), key=lambda x: (-x[1], x[0]))[:10]
    max_gap = max(deltas_min)
    print(f"DEBUG: interval(min) top10 = {top}")
    print(f"DEBUG: max gap(min) = {max_gap}")


def fetch_glucose_data() -> List[Dict[str, Any]]:
    email = get_env("LIBRELINK_EMAIL")
    password = get_env("LIBRELINK_PASSWORD")

    api = PyLibreLinkUp(email=email, password=password)

    # api_url は環境変数で上書き可能。JP固定にしたいならデフォルトをJPに。
    api.api_url = get_env_opt("LIBRELINK_API_URL", "https://api-jp.libreview.io")
    api.authenticate()

    patient, patient_id = _pick_patient(api)

    # 表示用（取れれば）
    first = getattr(patient, "first_name", "") if not isinstance(patient, dict) else patient.get("firstName", "")
    last = getattr(patient, "last_name", "") if not isinstance(patient, dict) else patient.get("lastName", "")
    label = (f"{first} {last}").strip() or str(patient)
    print(f"Patient: {label}")
    print(f"Patient ID: {patient_id}")

    graph_data = _fetch_graph(api, patient_id)
    logbook = _fetch_logbook(api, patient_id)

    assume_tz = get_env_opt("LIBRELINK_ASSUME_TZ", "Asia/Tokyo")

    all_measurements: List[Dict[str, Any]] = []

    for src_name, arr in (("graph", graph_data), ("logbook", logbook)):
        for m in arr:
            ts_raw = _get_timestamp(m)
            if ts_raw is None:
                continue
            ts_ms = _dt_to_epoch_ms(ts_raw, assume_tz)
            val, is_hi = _norm_value(m)
            all_measurements.append(
                {
                    "timestamp": ts_ms,
                    "value": val,
                    "isHi": bool(is_hi),
                    # sourceはデバッグに有用（不要なら消してOK）
                    "source": src_name,
                }
            )

    # タイムスタンプ(分)で重複除去：同一分に複数来たら「後勝ち」(logbookで上書き等)
    by_min: Dict[int, Dict[str, Any]] = {}
    for m in all_measurements:
        k = m["timestamp"] // 60000
        # 先にgraph→後にlogbookを入れているので、後勝ちでlogbook優先になる
        by_min[k] = m

    unique = list(by_min.values())
    unique.sort(key=lambda x: x["timestamp"])

    print(f"取得データ: graph={len(graph_data)}件 / logbook={len(logbook)}件 / unique={len(unique)}件")

    if get_env_opt("DEBUG_INTERVALS", "0") == "1":
        _debug_intervals(unique)

    # Firestoreに余計なフィールド不要ならsourceを落とす（今のHTMLは無視するので残しても害は薄い）
    for m in unique:
        m.pop("source", None)

    return unique


def write_to_firebase(db: firestore.Client, measurements: List[Dict[str, Any]]) -> int:
    if not measurements:
        return 0

    col = db.collection("glucose")

    min_ts = measurements[0]["timestamp"]
    max_ts = measurements[-1]["timestamp"]

    existing_docs = (
        col.where("timestamp", ">=", min_ts)
        .where("timestamp", "<=", max_ts)
        .get()
    )

    existing_minutes = set()
    for doc in existing_docs:
        d = doc.to_dict() or {}
        ts = d.get("timestamp")
        if ts is None:
            continue
        try:
            existing_minutes.add(int(ts) // 60000)
        except Exception:
            continue

    added = 0
    batch = db.batch()
    batch_count = 0

    for m in measurements:
        minute_key = m["timestamp"] // 60000
        if minute_key in existing_minutes:
            continue

        ref = col.document()
        batch.set(ref, m)
        added += 1
        batch_count += 1

        # Firestore batchは500上限。余裕をみて450でコミット
        if batch_count >= 450:
            batch.commit()
            batch = db.batch()
            batch_count = 0

    if batch_count > 0:
        batch.commit()

    return added


def main() -> None:
    print(f"実行時刻(UTC): {datetime.now(timezone.utc).isoformat()}")

    db = init_firebase()
    print("Firebase接続OK")

    measurements = fetch_glucose_data()
    if not measurements:
        print("新しいデータはありません")
        return

    added = write_to_firebase(db, measurements)
    print(f"結果: {added}件追加 / {len(measurements) - added}件スキップ（既存）")


if __name__ == "__main__":
    main()
