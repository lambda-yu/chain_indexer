from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass

from pydantic import TypeAdapter, ValidationError
from sqlalchemy import select

from apps.web.schemas import ArgFilterValue
from core.config.db import Database
from core.config.models import Subscription
from core.matcher.filters import FilterError, validate as _validate_filter_keys

_VALUE_SHAPE = TypeAdapter(dict[str, ArgFilterValue])


@dataclass(frozen=True)
class Offender:
    subscription_id: str
    name: str
    reason: str


async def scan_database(db: Database) -> list[Offender]:
    offenders: list[Offender] = []
    async with db.session() as s:
        result = await s.execute(select(Subscription))
        for row in result.scalars().all():
            try:
                _VALUE_SHAPE.validate_python(row.arg_filters)
                _validate_filter_keys(row.arg_filters)
            except ValidationError as exc:
                offenders.append(
                    Offender(
                        subscription_id=row.id,
                        name=row.name,
                        reason=f"value shape: {exc.errors()[0]['msg']}",
                    )
                )
            except FilterError as exc:
                offenders.append(
                    Offender(
                        subscription_id=row.id,
                        name=row.name,
                        reason=f"operator grammar: {exc}",
                    )
                )
    return offenders


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Scan subscriptions.arg_filters for M2-incompatible rows."
    )
    parser.add_argument(
        "--database-url",
        required=True,
        help="SQLAlchemy URL.",
    )
    args = parser.parse_args(argv)

    async def _run() -> int:
        db = Database(args.database_url)
        await db.connect()
        try:
            offenders = await scan_database(db)
        finally:
            await db.disconnect()

        if not offenders:
            print("0 offenders — all arg_filters rows pass the M2 schema")
            return 0

        print(f"{len(offenders)} offender(s):")
        for o in offenders:
            print(f"  - id={o.subscription_id} name={o.name!r} reason={o.reason}")
        return 1

    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
