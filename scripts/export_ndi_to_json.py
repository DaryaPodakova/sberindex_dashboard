"""
Выгрузка данных из sberindex.vw_ndi_calculation в JSON для дашборда.

Создает JSON файл со всеми 128 населенными пунктами и их компонентами NDI.

Использование:
    python export_ndi_to_json.py

Результат:
    sberindex_dashboard/dashboard/ndi_data.json
"""

import os
import sys
import json
from pathlib import Path
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# Fix stdout encoding for Windows console
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# Load environment
load_dotenv()

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_FILE = BASE_DIR / "dashboard" / "ndi_data.json"

# Database connection
_db_host = os.getenv('DB_HOST', 'localhost')
if _db_host == 'host.docker.internal':
    _db_host = 'localhost'

DB_PARAMS = {
    'host': _db_host,
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'platform'),
    'user': os.getenv('DB_USER', 'bot_etl_user2'),
    'password': os.getenv('DB_PASSWORD', 'allen'),
    'client_encoding': 'utf8',
}


def export_ndi_data():
    """Export NDI data from PostgreSQL VIEW to JSON."""
    print("=" * 80)
    print("📥 ЭКСПОРТ ДАННЫХ NDI В JSON")
    print("=" * 80)
    print()

    # Connect to PostgreSQL
    print("🔌 Подключаюсь к PostgreSQL...")
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Query VIEW
    query = """
    SELECT
        settlement_id,
        settlement_name,
        settlement_type,
        region_name,
        is_arctic,
        is_remote,
        is_special,
        is_suburb,
        poad_score,
        market_score,
        consumption_score,
        accessibility_score,
        climate_score,
        mobility_score,
        poad_score_100,
        market_score_100,
        consumption_score_100,
        accessibility_score_100,
        climate_score_100,
        mobility_score_100,
        ndi_score,
        ndi_score_100,
        ndi_10,
        ndi_rank,
        color_ndi,
        avg_temp_celsius,
        avg_temp_winter_celsius,
        avg_temp_summer_celsius,
        temp_amplitude_celsius,
        avg_hdd_yearly
    FROM sberindex.vw_ndi_calculation
    ORDER BY ndi_rank;
    """

    print("🔍 Запрос данных из sberindex.vw_ndi_calculation...")
    cursor.execute(query)
    rows = cursor.fetchall()

    print(f"✅ Получено {len(rows)} записей")

    # Convert Decimal to float for JSON serialization
    data = []
    for row in rows:
        record = {}
        for key, value in row.items():
            if value is None:
                record[key] = None
            elif isinstance(value, (int, str, bool)):
                record[key] = value
            else:
                # Convert Decimal to float
                record[key] = float(value)
        data.append(record)

    # Close connection
    cursor.close()
    conn.close()

    # Ensure output directory exists
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    # Write to JSON
    print(f"💾 Сохраняю данные в {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    file_size = OUTPUT_FILE.stat().st_size
    print(f"✅ Файл создан: {OUTPUT_FILE}")
    print(f"📊 Размер: {file_size:,} байт ({file_size / 1024:.1f} KB)")
    print()

    # Print sample
    print("📋 ПРИМЕР ДАННЫХ (первая запись):")
    print(json.dumps(data[0], ensure_ascii=False, indent=2))
    print()

    # Statistics
    print("=" * 80)
    print("📊 СТАТИСТИКА")
    print("=" * 80)
    print(f"Всего НП:           {len(data)}")
    print(f"Регионов:           {len(set(r['region_name'] for r in data))}")
    print(f"NDI min:            {min(r['ndi_10'] for r in data):.2f}")
    print(f"NDI max:            {max(r['ndi_10'] for r in data):.2f}")
    print(f"NDI средний:        {sum(r['ndi_10'] for r in data) / len(data):.2f}")
    print()

    print("=" * 80)
    print("✅ ГОТОВО! Данные экспортированы в JSON")
    print("=" * 80)
    print()
    print("📂 Путь к файлу:")
    print(f"   {OUTPUT_FILE}")
    print()
    print("🔗 Теперь можно подключить этот JSON к дашборду")
    print()

    return str(OUTPUT_FILE)


if __name__ == "__main__":
    try:
        output_path = export_ndi_data()
        sys.exit(0)
    except Exception as e:
        print()
        print("=" * 80)
        print("❌ ОШИБКА")
        print("=" * 80)
        print(f"{type(e).__name__}: {e}")
        sys.exit(1)
