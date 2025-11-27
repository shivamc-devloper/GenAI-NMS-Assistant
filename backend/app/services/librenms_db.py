import mysql.connector
from utils.config import DB_CONFIG


def get_db():
    return mysql.connector.connect(**DB_CONFIG)


# Memory Usage
def get_memory_usage(device_id: int):
    db = get_db()
    cursor = db.cursor(dictionary=True)

    cursor.execute(
        """
        SELECT mempool_descr, mempool_used, mempool_free, mempool_total, mempool_perc
        FROM mempools
        WHERE device_id = %s
    """,
        (device_id,),
    )

    rows = cursor.fetchall()
    db.close()

    if not rows:
        return None

    memory = []
    for r in rows:
        memory.append(
            {
                "type": r["mempool_descr"],
                "used_gb": round(r["mempool_used"] / (1024**3), 2),
                "free_gb": round(r["mempool_free"] / (1024**3), 2),
                "total_gb": round(r["mempool_total"] / (1024**3), 2),
                "usage_percent": r["mempool_perc"],
            }
        )

    return memory


# CPU usage
def get_cpu_usage(device_id: int):
    db = get_db()
    cursor = db.cursor(dictionary=True)

    cursor.execute(
        """
        SELECT processor_usage, processor_descr
        FROM processors
        WHERE device_id = %s
    """,
        (device_id,),
    )

    rows = cursor.fetchall()
    db.close()

    if not rows:
        return {"average": None, "cores": []}

    avg_cpu = sum(row["processor_usage"] for row in rows) / len(rows)

    return {
        "average": round(avg_cpu, 2),
        "cores": [
            {"core": r["processor_descr"], "usage": r["processor_usage"]} for r in rows
        ],
    }
