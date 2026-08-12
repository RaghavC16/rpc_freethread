"""Render control-plane comparison JSON as a dependency-free SVG chart."""

from __future__ import annotations

import argparse
import html
import json
from pathlib import Path
from typing import Any

WIDTH = 1400
HEIGHT = 940
LEFT = 110
RIGHT = 55
PLOT_WIDTH = WIDTH - LEFT - RIGHT

SERIES = (
    ("ray_regular", "Ray / GIL", "#d95f59"),
    ("nogil_rpc_regular", "nogil_rpc / GIL", "#3973ac"),
    ("nogil_rpc_free_threaded", "nogil_rpc / no-GIL", "#2d936c"),
)

RATIOS = (
    (
        "nogil_rpc_regular_throughput_vs_ray_regular",
        "RPC/GIL ÷ Ray/GIL",
        "#7868b4",
    ),
    (
        "nogil_rpc_free_threaded_throughput_vs_regular",
        "RPC/no-GIL ÷ RPC/GIL",
        "#e08b2c",
    ),
    (
        "nogil_rpc_free_threaded_throughput_vs_ray_regular",
        "RPC/no-GIL ÷ Ray/GIL",
        "#2d936c",
    ),
)


def _line(
    x1: float,
    y1: float,
    x2: float,
    y2: float,
    *,
    stroke: str,
    width: float = 1,
    dash: str | None = None,
) -> str:
    dashed = f' stroke-dasharray="{dash}"' if dash else ""
    return (
        f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
        f'stroke="{stroke}" stroke-width="{width}"{dashed}/>'
    )


def _text(
    x: float,
    y: float,
    value: str,
    *,
    size: int = 16,
    anchor: str = "start",
    color: str = "#263238",
    weight: str = "400",
) -> str:
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" font-family="Inter, Arial, sans-serif" '
        f'font-size="{size}" text-anchor="{anchor}" fill="{color}" '
        f'font-weight="{weight}">{html.escape(value)}</text>'
    )


def render(data: dict[str, Any]) -> str:
    comparisons = data["comparisons"]
    coordinators = [item["coordinators"] for item in comparisons]
    if not comparisons:
        raise ValueError("benchmark JSON has no comparisons")
    repetitions = data["repetitions"]
    python_version = comparisons[0]["ray_regular"]["python_version"]

    top_y, top_height = 155, 330
    bottom_y, bottom_height = 590, 245
    x_positions = {
        count: LEFT + index * PLOT_WIDTH / max(1, len(coordinators) - 1)
        for index, count in enumerate(coordinators)
    }

    raw_rates = [
        run["control_calls_per_second"]
        for item in comparisons
        for key, _, _ in SERIES
        for run in item["raw_runs"][key]
    ]
    top_max = max(raw_rates) * 1.12
    top_step = 1000.0
    top_max = max(top_step, ((top_max // top_step) + 1) * top_step)

    ratio_max = max(
        item[key] for item in comparisons for key, _, _ in RATIOS
    )
    ratio_step = 1.0
    ratio_max = max(2.0, ((ratio_max // ratio_step) + 1) * ratio_step)

    def x_value(count: int) -> float:
        return x_positions[count]

    def top_value(value: float) -> float:
        return top_y + top_height - value / top_max * top_height

    def ratio_value(value: float) -> float:
        return bottom_y + bottom_height - value / ratio_max * bottom_height

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" '
        f'height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}">',
        '<rect width="100%" height="100%" fill="#fbfcfe"/>',
        _text(
            LEFT,
            52,
            "Control-plane actor benchmark: Ray vs nogil_rpc",
            size=30,
            weight="700",
        ),
        _text(
            LEFT,
            82,
            f"Python {python_version} · median of {repetitions} runs · "
            "whiskers show min–max",
            size=17,
            color="#5f6b72",
        ),
        _text(LEFT, 128, "Steady-state throughput", size=21, weight="650"),
    ]

    for tick in range(0, int(top_max) + 1, int(top_step)):
        y = top_value(tick)
        svg.append(_line(LEFT, y, WIDTH - RIGHT, y, stroke="#dfe5e9"))
        svg.append(_text(LEFT - 14, y + 5, f"{tick:,}", anchor="end", size=14))
    svg.append(
        _text(
            28,
            top_y + top_height / 2,
            "control calls / second",
            anchor="middle",
            size=15,
        ).replace(
            "<text ",
            f'<text transform="rotate(-90 28 {top_y + top_height / 2:.1f})" ',
            1,
        )
    )

    for key, label, color in SERIES:
        points = []
        for item in comparisons:
            x = x_value(item["coordinators"])
            median_rate = item[key]["control_calls_per_second"]
            raw = [
                run["control_calls_per_second"]
                for run in item["raw_runs"][key]
            ]
            low_y, high_y = top_value(min(raw)), top_value(max(raw))
            svg.extend(
                (
                    _line(x, high_y, x, low_y, stroke=color, width=2),
                    _line(x - 7, high_y, x + 7, high_y, stroke=color, width=2),
                    _line(x - 7, low_y, x + 7, low_y, stroke=color, width=2),
                )
            )
            points.append((x, top_value(median_rate), median_rate))
        svg.append(
            '<polyline fill="none" '
            f'stroke="{color}" stroke-width="4" stroke-linejoin="round" '
            f'points="{" ".join(f"{x:.1f},{y:.1f}" for x, y, _ in points)}"/>'
        )
        for x, y, value in points:
            svg.append(
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="7" '
                f'fill="{color}" stroke="#ffffff" stroke-width="2"/>'
            )
            svg.append(
                _text(
                    x,
                    y - 13,
                    f"{value:,.0f}",
                    anchor="middle",
                    size=13,
                    color=color,
                    weight="650",
                )
            )

    legend_x = LEFT + 25
    for index, (_, label, color) in enumerate(SERIES):
        x = legend_x + index * 285
        svg.append(_line(x, 110, x + 35, 110, stroke=color, width=4))
        svg.append(f'<circle cx="{x + 17.5:.1f}" cy="110" r="6" fill="{color}"/>')
        svg.append(_text(x + 45, 116, label, size=15, weight="600"))

    svg.append(_text(LEFT, 555, "Median throughput ratios", size=21, weight="650"))
    for tick in range(0, int(ratio_max) + 1):
        y = ratio_value(tick)
        svg.append(_line(LEFT, y, WIDTH - RIGHT, y, stroke="#dfe5e9"))
        svg.append(_text(LEFT - 14, y + 5, f"{tick}×", anchor="end", size=14))
    one_y = ratio_value(1.0)
    svg.append(_line(LEFT, one_y, WIDTH - RIGHT, one_y, stroke="#7b878d", width=2, dash="7 6"))

    for key, label, color in RATIOS:
        points = [
            (
                x_value(item["coordinators"]),
                ratio_value(item[key]),
                item[key],
            )
            for item in comparisons
        ]
        svg.append(
            '<polyline fill="none" '
            f'stroke="{color}" stroke-width="4" stroke-linejoin="round" '
            f'points="{" ".join(f"{x:.1f},{y:.1f}" for x, y, _ in points)}"/>'
        )
        for x, y, value in points:
            svg.append(
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="6" '
                f'fill="{color}" stroke="#ffffff" stroke-width="2"/>'
            )
            svg.append(
                _text(
                    x,
                    y - 12,
                    f"{value:.2f}×",
                    anchor="middle",
                    size=13,
                    color=color,
                    weight="650",
                )
            )

    for index, (_, label, color) in enumerate(RATIOS):
        x = legend_x + index * 355
        svg.append(_line(x, 538, x + 35, 538, stroke=color, width=4))
        svg.append(_text(x + 45, 544, label, size=14, weight="600"))

    for count in coordinators:
        x = x_value(count)
        svg.append(_text(x, 866, str(count), anchor="middle", size=16, weight="600"))
    svg.append(
        _text(
            LEFT + PLOT_WIDTH / 2,
            900,
            "concurrent coordinators",
            anchor="middle",
            size=17,
            weight="600",
        )
    )
    svg.append(
        _text(
            WIDTH - RIGHT,
            925,
            "Work grows with coordinator count: 200 rounds/coordinator, batch 8",
            anchor="end",
            size=13,
            color="#6d787e",
        )
    )
    svg.append("</svg>")
    return "\n".join(svg)


def render_png(data: dict[str, Any], output: Path) -> None:
    """Render a raster copy when Matplotlib is available."""
    import matplotlib.pyplot as plt

    comparisons = data["comparisons"]
    coordinators = [item["coordinators"] for item in comparisons]
    repetitions = data["repetitions"]
    sample = comparisons[0]["ray_regular"]
    figure, (throughput, ratios) = plt.subplots(
        2,
        1,
        figsize=(14, 9),
        gridspec_kw={"height_ratios": [1.35, 1]},
        constrained_layout=True,
    )
    figure.suptitle(
        "Control-plane actor benchmark: Ray vs nogil_rpc",
        fontsize=20,
        fontweight="bold",
    )

    for key, label, color in SERIES:
        medians = [
            item[key]["control_calls_per_second"] for item in comparisons
        ]
        raw = [
            [run["control_calls_per_second"] for run in item["raw_runs"][key]]
            for item in comparisons
        ]
        errors = [
            [median_value - min(values) for median_value, values in zip(medians, raw)],
            [max(values) - median_value for median_value, values in zip(medians, raw)],
        ]
        throughput.errorbar(
            coordinators,
            medians,
            yerr=errors,
            label=label,
            color=color,
            marker="o",
            linewidth=2.5,
            markersize=7,
            capsize=5,
        )
        for x, y in zip(coordinators, medians):
            throughput.annotate(
                f"{y:,.0f}",
                (x, y),
                xytext=(0, 10),
                textcoords="offset points",
                ha="center",
                color=color,
                fontsize=9,
                fontweight="bold",
            )

    throughput.set_title(
        "Steady-state throughput (median; whiskers show min–max)",
        loc="left",
        fontsize=14,
        fontweight="bold",
    )
    throughput.set_ylabel("Control calls / second")
    throughput.set_xticks(coordinators)
    throughput.grid(axis="y", alpha=0.25)
    throughput.legend(ncol=3, frameon=False, loc="upper left")

    for key, label, color in RATIOS:
        values = [item[key] for item in comparisons]
        ratios.plot(
            coordinators,
            values,
            label=label,
            color=color,
            marker="o",
            linewidth=2.5,
            markersize=7,
        )
        for x, y in zip(coordinators, values):
            ratios.annotate(
                f"{y:.2f}×",
                (x, y),
                xytext=(0, 9),
                textcoords="offset points",
                ha="center",
                color=color,
                fontsize=9,
                fontweight="bold",
            )

    ratios.axhline(1, color="#7b878d", linestyle="--", linewidth=1.5)
    ratios.set_title("Median throughput ratios", loc="left", fontsize=14, fontweight="bold")
    ratios.set_xlabel("Concurrent coordinators")
    ratios.set_ylabel("Throughput ratio")
    ratios.set_xticks(coordinators)
    ratios.grid(axis="y", alpha=0.25)
    ratios.legend(ncol=3, frameon=False, loc="upper left")
    figure.text(
        0.99,
        0.005,
        f"Python {sample['python_version']} · {repetitions} repetitions · "
        f"{sample['rounds_per_coordinator']} rounds/coordinator · "
        f"batch size {sample['batch_size']}",
        ha="right",
        fontsize=9,
        color="#5f6b72",
    )
    figure.savefig(output, dpi=170, facecolor="#fbfcfe")
    plt.close(figure)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    data = json.loads(args.input.read_text())
    if args.output.suffix.lower() == ".png":
        render_png(data, args.output)
    else:
        args.output.write_text(render(data))
    print(args.output)


if __name__ == "__main__":
    main()
