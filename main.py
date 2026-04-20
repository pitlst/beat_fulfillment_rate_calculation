import asyncio
import clickhouse_connect
import clickhouse_connect.driver.asyncclient
from clickhouse_connect.driver import httputil
import datetime
import pandas as pd
import time
import warnings
import traceback
from functools import lru_cache  # 改用标准库 lru_cache，不需要 async 版
from tqdm import tqdm
import concurrent.futures  # 引入线程池

warnings.simplefilter("ignore", FutureWarning)

DAILY_PERIODS = [(8, 0, 12, 0), (13, 30, 17, 30)]  # 提到模块级，避免重复构造


def get_clickhouse_type(dtype) -> str:  # type: ignore
    """根据 pandas dtype 推断 ClickHouse 类型"""
    if pd.api.types.is_integer_dtype(dtype):
        return 'Int64'
    if pd.api.types.is_float_dtype(dtype):
        return 'Float64'
    if pd.api.types.is_bool_dtype(dtype):
        return 'UInt8'  # ClickHouse 中常用 UInt8 表示布尔
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DateTime64(6)'
    # 默认字符串，能兼容中文、混合类型等
    return 'String'


async def create_table_from_df(
    client,
    df: pd.DataFrame,
    table_name: str,
    database: str = "default"
):
    """根据 DataFrame 自动生成并执行 CREATE TABLE"""
    columns_def = []
    for col_name, dtype in df.dtypes.items():
        ch_type = get_clickhouse_type(dtype)
        # 中文/特殊列名用双引号包裹，避免 ClickHouse 解析错误
        safe_col = f'"{col_name}"'
        columns_def.append(f"    {safe_col} {ch_type}")

    # 如果列很多，用 tuple() 作为 ORDER BY（无排序键）
    # 如果有合适的列（如 ID、时间），可以改成 ORDER BY ("xxx")
    sql = f"""CREATE TABLE IF NOT EXISTS {database}.{table_name} (
{',\n'.join(columns_def)}
) ENGINE = MergeTree()
ORDER BY tuple()
"""
    await client.command(sql)
    print(f"已确保表存在: {database}.{table_name} ({len(df.columns)} 列)")

_client = None


async def get_client() -> clickhouse_connect.driver.asyncclient.AsyncClient:
    global _client
    if _client is None:
        pool_mgr = httputil.get_pool_manager(maxsize=32, num_pools=2, block=True, timeout=300)
        _client = await clickhouse_connect.get_async_client(
            host="10.24.5.59", port=8123, username="cheakf", password="Swq8855830.",
            database="default", pool_mgr=pool_mgr
        )
    return _client


def normalize_datetime(dt: datetime.datetime) -> datetime.datetime:
    if dt.tzinfo is not None:
        return dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return dt

# ========== 核心改动：全部改为同步函数，去掉 async/await 开销 ==========


def is_workday(d: datetime.date, workday_cache: dict) -> bool:
    """纯内存判断，无需 async"""
    if d in workday_cache:
        return workday_cache[d]
    return d.weekday() < 5


@lru_cache(maxsize=8192)  # 缓存加大，因为日期组合有限
def get_worktime(start_time: datetime.datetime, end_time: datetime.datetime) -> int:
    """纯 CPU 计算，改为同步，使用标准 lru_cache"""
    start_time = normalize_datetime(start_time)
    end_time = normalize_datetime(end_time)
    reverse = False
    if start_time >= end_time:
        reverse = True
        end_time, start_time = start_time, end_time

    total_seconds = 0
    current_date = start_time.date()
    end_date = end_time.date()

    while current_date <= end_date:
        if is_workday(current_date, _WORKDAY_CACHE):  # 使用模块级缓存
            for sh, sm, eh, em in DAILY_PERIODS:
                period_start = datetime.datetime.combine(current_date, datetime.time(sh, sm))
                period_end = datetime.datetime.combine(current_date, datetime.time(eh, em))
                intersect_start = max(start_time, period_start)
                intersect_end = min(end_time, period_end)
                if intersect_start < intersect_end:
                    total_seconds += (intersect_end - intersect_start).total_seconds()
        current_date += datetime.timedelta(days=1)

    return int(total_seconds // 60) if not reverse else -int(total_seconds // 60)


def process_row(row: pd.Series) -> pd.Series:
    """改为纯同步函数"""
    schedule_time = get_worktime(pd.to_datetime(row["排程开始时间"]), pd.to_datetime(row["排程结束时间"]))
    plan_time = get_worktime(pd.to_datetime(row["计划开始时间"]), pd.to_datetime(row["计划结束时间"]))

    if row["当前工序状态"] != '已完工':
        actual_time = -1
    else:
        actual_time = get_worktime(pd.to_datetime(row["实际开始时间"]), pd.to_datetime(row["实际结束时间"]))

    if actual_time == -1:
        fulfill = '工序未完工'
    elif actual_time <= schedule_time:
        fulfill = '是'
    else:
        fulfill = '否'

    if actual_time == -1:
        on_time = '工序未完工'
    elif (abs(get_worktime(pd.to_datetime(row["计划开始时间"]), pd.to_datetime(row["实际开始时间"]))) <= 240 and
          abs(get_worktime(pd.to_datetime(row["计划结束时间"]), pd.to_datetime(row["实际结束时间"]))) <= 240):
        on_time = '是'
    else:
        on_time = '否'

    return pd.Series({
        "排程执行时间": schedule_time,
        "计划执行时间": plan_time,
        "实际执行时间": actual_time,
        "是否兑现节拍": fulfill,
        "是否准时开完工": on_time
    })


def process_batch(rows: pd.DataFrame) -> pd.DataFrame:
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(process_row, row) for _, row in rows.iterrows()]
        results = [f.result() for f in futures]
    return pd.DataFrame(results)


# 模块级变量，用于跨函数共享（进程池方案需要，这里先准备好）
_WORKDAY_CACHE = {}


async def main_one():
    global _WORKDAY_CACHE
    client = await get_client()

    # 1. 预加载节假日（保持 async IO）
    holiday_df = await client.query_df("""
        SELECT toDate("节假日日期") as d, "是否休息" as is_rest 
        FROM ods.attendance_kq_scheduling_holiday
        WHERE Deleted = 0 
        QUALIFY row_number() OVER (PARTITION BY zid ORDER BY Version DESC) = 1
    """)
    _WORKDAY_CACHE.clear()
    for _, row in holiday_df.iterrows():
        _WORKDAY_CACHE[row['d']] = not bool(row['is_rest'])

    # 2. 加载主数据
    total_data = await client.query_df("SELECT * FROM dwd.process_cycle_time")
    print(f"获取到 {len(total_data)} 条数据")

    # 3. 分批 + 线程池处理
    batch_size = 500
    all_results = []

    with tqdm(total=len(total_data), desc="计算节拍兑现率", unit="条") as pbar:
        for i in range(0, len(total_data), batch_size):
            batch = total_data.iloc[i:i+batch_size]
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                process_batch,
                batch
            )
            all_results.append(result)
            pbar.update(len(batch))

    # 4. 写回数据库
    res_new_df = pd.concat(all_results, axis=0)
    # 关键修复：横向拼接前重置两边索引，避免重复索引报错
    res_new_df = res_new_df.reset_index(drop=True)
    total_data = total_data.reset_index(drop=True)
    res_df = pd.concat([res_new_df, total_data], axis=1)

    await client.command("DROP TABLE IF EXISTS dwd.beat_fulfillment_rate")
    await create_table_from_df(client, res_df, "beat_fulfillment_rate", "dwd")
    await client.insert_df("beat_fulfillment_rate", res_df, "dwd")


async def main():
    while True:
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 节拍兑现率开始计算")
        try:
            await main_one()
        except Exception:
            print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] error:{traceback.format_exc()}")
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 节拍兑现率计算完成，等待1分钟......")
        time.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
