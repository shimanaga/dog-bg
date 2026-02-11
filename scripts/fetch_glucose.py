"""
LibreLinkUp → Firebase 血糖値データ取得スクリプト

GitHub Actionsから15分ごとに実行され、
LibreLinkUpのAPIからグルコースデータを取得してFirestoreに書き込む。

必要な環境変数:
  LIBRELINK_EMAIL       - LibreLinkUpのログインメールアドレス
  LIBRELINK_PASSWORD    - LibreLinkUpのパスワード
  FIREBASE_SERVICE_ACCOUNT - Firebaseサービスアカウントキー(JSON文字列)
"""

import os
import sys
import json
from datetime import datetime, timezone

import firebase_admin
from firebase_admin import credentials, firestore
from pylibrelinkup import PyLibreLinkUp


def get_env(name: str) -> str:
    """環境変数を取得。未設定ならエラー終了。"""
    val = os.environ.get(name, "").strip()
    if not val:
        print(f"ERROR: 環境変数 {name} が設定されていません")
        sys.exit(1)
    return val


def init_firebase() -> firestore.Client:
    """Firebaseを初期化してFirestoreクライアントを返す。"""
    sa_json = get_env("FIREBASE_SERVICE_ACCOUNT")
    sa_dict = json.loads(sa_json)
    cred = credentials.Certificate(sa_dict)
    firebase_admin.initialize_app(cred)
    return firestore.client()


def fetch_glucose_data() -> list:
    """LibreLinkUpからグルコースデータを取得。"""
    email = get_env("LIBRELINK_EMAIL")
    password = get_env("LIBRELINK_PASSWORD")

    api = PyLibreLinkUp(email=email, password=password)
    api.api_url = "https://api-jp.libreview.io"
    api.authenticate()

    patients = api.get_patients()
    if not patients:
        print("WARNING: 患者（ペット）が見つかりません")
        return []

    patient = patients[0]
    print(f"Patient: {patient.first_name} {patient.last_name}")

    # グラフデータ（直近12-24時間、15分間隔）
    graph_data = api.get_graph_data(patient)

    # Logbook（より長期の履歴、スキャン時の値）
    logbook = api.get_logbook(patient)

    # 両方を統合
    all_measurements = []

    for m in graph_data:
        ts_ms = int(m.timestamp.replace(tzinfo=timezone.utc).timestamp() * 1000)
        all_measurements.append({
            "timestamp": ts_ms,
            "value": m.value,
            "isHi": m.value >= 500,
        })

    for m in logbook:
        ts_ms = int(m.timestamp.replace(tzinfo=timezone.utc).timestamp() * 1000)
        all_measurements.append({
            "timestamp": ts_ms,
            "value": m.value,
            "isHi": m.value >= 500,
        })

    # タイムスタンプで重複除去（同じ分のデータは1つだけ）
    seen = set()
    unique = []
    for m in all_measurements:
        key = (m["timestamp"] // 60000, m["value"])
        if key not in seen:
            seen.add(key)
            unique.append(m)

    unique.sort(key=lambda x: x["timestamp"])
    print(f"取得データ: {len(unique)}件")
    return unique


def write_to_firebase(db: firestore.Client, measurements: list) -> int:
    """Firestoreにデータを書き込む。既存データと重複するものはスキップ。"""
    if not measurements:
        return 0

    col = db.collection("glucose")

    # 取得範囲の既存データを取得して重複チェック
    min_ts = measurements[0]["timestamp"]
    max_ts = measurements[-1]["timestamp"]

    existing_docs = (
        col.where("timestamp", ">=", min_ts)
        .where("timestamp", "<=", max_ts)
        .get()
    )
    existing_keys = set()
    for doc in existing_docs:
        d = doc.to_dict()
        ts = d.get("timestamp")
        val = d.get("value")
        if ts is not None and val is not None:
            existing_keys.add((ts // 60000, val))

    # 新規データだけ書き込み
    added = 0
    batch = db.batch()
    batch_count = 0

    for m in measurements:
        key = (m["timestamp"] // 60000, m["value"])
        if key in existing_keys:
            continue

        ref = col.document()
        batch.set(ref, m)
        added += 1
        batch_count += 1

        # Firestoreのバッチは500件まで
        if batch_count >= 450:
            batch.commit()
            batch = db.batch()
            batch_count = 0

    if batch_count > 0:
        batch.commit()

    return added


def main():
    print(f"実行時刻: {datetime.now(timezone.utc).isoformat()}")

    # Firebase初期化
    db = init_firebase()
    print("Firebase接続OK")

    # LibreLinkUpからデータ取得
    measurements = fetch_glucose_data()

    if not measurements:
        print("新しいデータはありません")
        return

    # Firebaseに書き込み
    added = write_to_firebase(db, measurements)
    print(f"結果: {added}件追加 / {len(measurements) - added}件スキップ（既存）")


if __name__ == "__main__":
    main()
