import asyncio
import clickhouse_connect
import clickhouse_connect.driver.asyncclient
from clickhouse_connect.driver import httputil
import datetime
import pandas as pd
import time
import warnings
import traceback
from async_lru import alru_cache
from tqdm import tqdm

warnings.simplefilter("ignore", FutureWarning)

def normalize_datetime(dt: datetime.datetime) -> datetime.datetime:
    if dt.tzinfo is not None:
        # 先转 UTC（或你需要的时区），再去掉时区
        return dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return dt

_client = None


async def get_client() -> clickhouse_connect.driver.asyncclient.AsyncClient:
    """根据参数创建 ClickHouse 客户端."""
    global _client
    if _client is None:
        pool_mgr = httputil.get_pool_manager(
            maxsize=32,
            num_pools=2,
            block=True,
            timeout=300
        )
        _client = await clickhouse_connect.get_async_client(
            host="10.24.5.59",
            port=8123,
            username="cheakf",
            password="Swq8855830.",
            database="default",
            pool_mgr=pool_mgr
        )
    return _client


async def main_one():
    client = await get_client()
    holiday_df = await client.query_df(
        f"""
SELECT 
    toDate("节假日日期") as d, 
    "是否休息" as is_rest 
FROM ods.attendance_kq_scheduling_holiday
    WHERE Deleted = 0 
    QUALIFY row_number() OVER (PARTITION BY zid ORDER BY Version DESC) = 1
        """
    )
    workday_cache: dict[datetime.datetime, bool] = {}
    for _, row in holiday_df.iterrows():
        workday_cache[row['d']] = not bool(row['is_rest'])

    async def is_workday(input_value: datetime.datetime) -> bool:
        '''获取是否是工作日'''
        if input_value in workday_cache:
            return workday_cache[input_value]
        return input_value.weekday() < 5

    @alru_cache(maxsize=1024)
    async def get_worktime(start_time: datetime.datetime, end_time: datetime.datetime) -> int:
        '''获取工作时间，结果为分钟'''
        start_time = normalize_datetime(start_time)
        end_time = normalize_datetime(end_time)
        reverse = False
        if start_time >= end_time:
            reverse = True
            end_time, start_time = start_time, end_time
        total_seconds = 0
        current_date = start_time.date()
        end_date = end_time.date()
        # 每天的工作时段配置：(开始小时, 开始分钟, 结束小时, 结束分钟)
        daily_periods: list[tuple[int, int, int, int]] = [
            (8, 0, 12, 0),    # 上午 8:00 - 12:00
            (13, 30, 17, 30)  # 下午 13:30 - 17:30
        ]
        while current_date <= end_date:
            # 构造当天的 datetime 用于判断是否为工作日
            check_dt = datetime.datetime.combine(current_date, datetime.time.min)
            if await is_workday(check_dt):
                for sh, sm, eh, em in daily_periods:
                    period_start = datetime.datetime.combine(current_date, datetime.time(sh, sm))
                    period_end = datetime.datetime.combine(current_date, datetime.time(eh, em))
                    intersect_start = max(start_time, period_start)
                    intersect_end = min(end_time, period_end)
                    if intersect_start < intersect_end:
                        total_seconds += (intersect_end - intersect_start).total_seconds()
            current_date += datetime.timedelta(days=1)
        return int(total_seconds // 60) if not reverse else -int(total_seconds // 60)

    total_data = await client.query_df(f"SELECT * FROM dwd.process_cycle_time")
    print(f"获取到{len(total_data)}条数据")

    async def process_row(row: pd.Series):
        schedule_time = await get_worktime(pd.to_datetime(row["排程开始时间"]), pd.to_datetime(row["排程结束时间"]))
        plan_time = await get_worktime(pd.to_datetime(row["计划开始时间"]), pd.to_datetime(row["计划结束时间"]))
        actual_time = -1 if row["当前工序状态"] != '已完工' else await get_worktime(pd.to_datetime(row["实际开始时间"]), pd.to_datetime(row["实际结束时间"]))
        if actual_time != -1:
            fulfill = '工序未完工'
        elif actual_time <= schedule_time:
            fulfill = '是'
        else:
            fulfill = '否'
        if actual_time != -1:
            on_time = '工序未完工'
        elif abs(await get_worktime(pd.to_datetime(row["计划开始时间"]), pd.to_datetime(row["实际开始时间"]))) <= 240 and abs(await get_worktime(pd.to_datetime(row["计划结束时间"]), pd.to_datetime(row["实际结束时间"]))) <= 240:
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
    async def process_batch(rows: pd.DataFrame):
        tasks = [process_row(row) for _, row in rows.iterrows()]
        results = await asyncio.gather(*tasks)
        return pd.concat(results, axis=0)
    
    batch_size = 500  # 每批 500 条
    all_results = []
    for i in tqdm(range(0, len(total_data), batch_size), desc="处理批次"):
        batch = total_data.iloc[i:i+batch_size]
        result = await process_batch(batch)
        all_results.append(result)
        await asyncio.sleep(0.5)
    res_new_df = pd.concat(all_results, axis=0)
    res_df = pd.concat([res_new_df, total_data], axis=1)
    await client.command("DROP TABLE IF EXISTS dwd.beat_fulfillment_rate")
    await client.insert_df("beat_fulfillment_rate", res_df, "dwd")


async def main():
    while True:
        print(f"[{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 节拍兑现率开始计算")
        try:
            await main_one()
        except Exception:
            print(f"[{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] error:{traceback.format_exc()}")
        # 等待1分钟
        print(f"[{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 节拍兑现率计算完成，等待1分钟......")
        time.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
