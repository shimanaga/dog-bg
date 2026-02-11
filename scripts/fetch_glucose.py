from datetime import timezone
from pylibrelinkup import PyLibreLinkUp
from pylibrelinkup.api_url import APIUrl
from pylibrelinkup.exceptions import RedirectError

def fetch_glucose_data() -> list:
    email = get_env("LIBRELINK_EMAIL")
    password = get_env("LIBRELINK_PASSWORD")

    api = PyLibreLinkUp(email=email, password=password, api_url=APIUrl.JP)
    try:
        api.authenticate()
    except RedirectError as e:
        new_region = e.args[0] if e.args else APIUrl.JP
        api = PyLibreLinkUp(email=email, password=password, api_url=new_region)
        api.authenticate()

    patients = api.get_patients()
    if not patients:
        print("WARNING: 患者（ペット）が見つかりません")
        return []

    patient = patients[0]
    print(f"Patient: {patient.first_name} {patient.last_name}")

    graph_data = api.graph(patient_identifier=patient)
    logbook = api.logbook(patient_identifier=patient)

    all_measurements = []

    def to_ms(dt):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

    for m in graph_data:
        all_measurements.append({
            "timestamp": to_ms(m.timestamp),
            "value": m.value,
            "isHi": m.value >= 500,
        })

    for m in logbook:
        all_measurements.append({
            "timestamp": to_ms(m.timestamp),
            "value": m.value,
            "isHi": m.value >= 500,
        })

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
