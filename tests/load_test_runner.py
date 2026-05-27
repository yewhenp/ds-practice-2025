#!/usr/bin/env python3
import asyncio
from pathlib import Path
from time import perf_counter

import httpx
import pandas as pd
import matplotlib.pyplot as plt


BASE_URL = "http://localhost:8081"
TIMEOUT_SEC = 120
RUNS_PER_CASE = 10
REQUEST_COUNTS = [2, 4, 8, 16, 32]


async def run_checkout(
    client: httpx.AsyncClient,
    url: str,
    case_size: int,
    run: int,
    request_id: int,
    start: asyncio.Event,
) -> dict:
    book = "Book A" if request_id % 2 else "Book B"
    payload = {
        "user": {
            "name": f"LoadUser-{case_size}-{run}-{request_id}",
            "contact": f"load.{case_size}.{run}.{request_id}@example.com",
        },
        "creditCard": {"number": "4111111111111111", "expirationDate": "12/27", "cvv": "123"},
        "userComment": f"Load test case={case_size}, run={run}, request={request_id}.",
        "items": [{"name": book, "quantity": 1}],
        "billingAddress": {
            "street": "123 Main St",
            "city": "Springfield",
            "state": "IL",
            "zip": "62701",
            "country": "USA",
        },
        "shippingMethod": "Standard",
        "giftWrapping": False,
        "termsAccepted": True,
    }
    await start.wait()
    started = perf_counter()

    try:
        await client.post(url, json=payload)
    except httpx.TimeoutException:
        pass

    return {
        "elapsed_ms": (perf_counter() - started) * 1000.0,
    }


def latency_metrics(latencies: pd.Series) -> dict:
    ordered = latencies.sort_values(ignore_index=True)
    fastest = ordered.iloc[0]
    adjacent_gaps = ordered.diff().dropna()
    diffs_from_fastest = ordered.iloc[1:] - fastest
    return {
        "mean_time_per_request": ordered.mean(),
        "std_time_per_request": ordered.std(ddof=0) if len(ordered) > 1 else 0.0,
        "max_request_gap": ordered.iloc[-1] - fastest,
        "min_request_gap": adjacent_gaps.min() if not adjacent_gaps.empty else 0.0,
        "mean_request_diff_from_fastest": diffs_from_fastest.mean() if not diffs_from_fastest.empty else 0.0,
        "std_request_diff_from_fastest": diffs_from_fastest.std(ddof=0) if len(diffs_from_fastest) > 1 else 0.0,
    }


async def run_case(
    client: httpx.AsyncClient,
    case_size: int,
    run: int,
) -> dict:
    start = asyncio.Event()
    url = f"{BASE_URL.rstrip('/')}/checkout"
    tasks = [
        asyncio.create_task(
            run_checkout(client, url, case_size, run, request_id, start)
        )
        for request_id in range(1, case_size + 1)
    ]

    await asyncio.sleep(0)
    start.set()
    results = pd.DataFrame(await asyncio.gather(*tasks))

    row = {
        "case_request_count": case_size,
        "run_index": run,
    }
    row.update(latency_metrics(results["elapsed_ms"]))
    return row


async def run_all() -> pd.DataFrame:
    rows = []
    timeout = httpx.Timeout(TIMEOUT_SEC)
    limits = httpx.Limits(max_connections=None, max_keepalive_connections=None)

    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        for case_size in REQUEST_COUNTS:
            for run in range(1, RUNS_PER_CASE + 1):
                print(f"[load] case={case_size} run={run}/{RUNS_PER_CASE}", flush=True)
                row = await run_case(client, case_size, run)
                rows.append(row)
                print(
                    f"[load] mean_ms={row['mean_time_per_request']} "
                    f"std_ms={row['std_time_per_request']} "
                    f"gap_ms={row['max_request_gap']}",
                    flush=True,
                )

    return pd.DataFrame(rows)


def summarize(runs: pd.DataFrame) -> pd.DataFrame:
    summary = runs.groupby("case_request_count", as_index=False).agg(
        mean_time_per_request=("mean_time_per_request", "mean"),
        std_time_per_request=("std_time_per_request", "mean"),
        max_request_gap=("max_request_gap", "mean"),
        min_request_gap=("min_request_gap", "mean"),
        mean_request_diff_from_fastest=("mean_request_diff_from_fastest", "mean"),
        std_request_diff_from_fastest=("std_request_diff_from_fastest", "mean"),
    )
    summary[summary.select_dtypes(include="number").columns] = summary.select_dtypes(include="number").round(4)
    return summary


def write_plot(summary: pd.DataFrame, metric: str, plots_dir: Path) -> Path:
    path = plots_dir / f"{metric}.png"
    fig, ax = plt.subplots(figsize=(9.5, 5.4), constrained_layout=True)
    ax.plot(summary["case_request_count"], summary[metric], marker="o", linewidth=2)
    ax.set_xscale("log")
    ax.set_title(metric.replace("_", " "))
    ax.set_xlabel("simultaneous request count")
    ax.set_ylabel("milliseconds")
    ax.grid(True, alpha=0.3)
    fig.savefig(path, dpi=160)
    plt.close(fig)
    return path


def write_plots(summary: pd.DataFrame) -> list[Path]:
    plots_dir = Path("tests/load_tests/plots")
    plots_dir.mkdir(parents=True, exist_ok=True)
    return [
        write_plot(summary, "mean_time_per_request", plots_dir),
        write_plot(summary, "std_time_per_request", plots_dir),
        write_plot(summary, "max_request_gap", plots_dir),
        write_plot(summary, "min_request_gap", plots_dir),
        write_plot(summary, "mean_request_diff_from_fastest", plots_dir),
        write_plot(summary, "std_request_diff_from_fastest", plots_dir),
    ]


async def main_async() -> list[Path]:
    runs = await run_all()
    summary = summarize(runs)

    Path("tests/load_tests").mkdir(parents=True, exist_ok=True)
    runs.to_csv("tests/load_tests/load_test_runs.csv", index=False)
    summary.to_csv("tests/load_tests/load_test_summary.csv", index=False)
    return write_plots(summary)


def main() -> None:
    plot_paths = asyncio.run(main_async())
    print("Run CSV written to tests/load_tests/load_test_runs.csv")
    print("Summary CSV written to tests/load_tests/load_test_summary.csv")
    print("Plots written to tests/load_tests/plots")
    for path in plot_paths:
        print(f"- {path}")


if __name__ == "__main__":
    main()
