from __future__ import annotations

import csv
from pathlib import Path
from typing import List

from databend_py import Connection
from harlequin import HarlequinCompletion


def _get_completions(conn: Connection) -> List[HarlequinCompletion]:
    completions: List[HarlequinCompletion] = []

    keyword_path = Path(__file__).parent / "keywords.tsv"
    with open(keyword_path, "r") as f:
        keyword_reader = csv.reader(f, delimiter="\t")
        for keyword, kind, _, _, _ in keyword_reader:
            completions.append(
                HarlequinCompletion(
                    label=keyword.lower(),
                    type_label="kw",
                    value=keyword.lower(),
                    priority=100 if kind.startswith("reserved") else 1000,
                    context=None,
                )
            )

    results = conn.query_with_session(
        """
        SELECT
        name as label,
        CASE
            WHEN is_aggregate = TRUE THEN 'agg'
            ELSE 'fn'
        END AS type_label
    FROM
        system.functions;
        """
    )
    for label, type_label in results:
        completions.append(
            HarlequinCompletion(
                label=label,
                type_label=type_label,
                value=label,
                priority=1000,
                context=None,
            )
        )
    return sorted(completions)
