from datetime import datetime
from decimal import Decimal
from typing import Dict, Any
from zoneinfo import ZoneInfo

from sqlalchemy import text


async def _get_property_timezone(session, property_id: str, tenant_id: str) -> str:
    row = await session.execute(
        text("SELECT timezone FROM properties WHERE id = :pid AND tenant_id = :tid"),
        {"pid": property_id, "tid": tenant_id},
    )
    result = row.fetchone()
    return result.timezone if result and result.timezone else "UTC"


async def calculate_monthly_revenue(
    property_id: str, tenant_id: str, month: int, year: int
) -> Decimal:
    """
    Calculates revenue for a specific month, in the property's local timezone.
    """
    from app.core.database_pool import db_pool
    await db_pool.initialize()

    async with db_pool.session_factory() as session:
        tz_name = await _get_property_timezone(session, property_id, tenant_id)
        tz = ZoneInfo(tz_name)

        start_local = datetime(year, month, 1, tzinfo=tz)
        if month < 12:
            end_local = datetime(year, month + 1, 1, tzinfo=tz)
        else:
            end_local = datetime(year + 1, 1, 1, tzinfo=tz)

        query = text(
            """
            SELECT COALESCE(SUM(total_amount), 0) AS total
            FROM reservations
            WHERE property_id = :pid
              AND tenant_id = :tid
              AND check_in_date >= :start
              AND check_in_date <  :end
            """
        )
        result = await session.execute(
            query,
            {"pid": property_id, "tid": tenant_id, "start": start_local, "end": end_local},
        )
        row = result.fetchone()
        return Decimal(str(row.total)) if row else Decimal("0")


async def calculate_total_revenue(property_id: str, tenant_id: str) -> Dict[str, Any]:
    """
    Aggregates total revenue for a property within a tenant.
    """
    from app.core.database_pool import db_pool
    await db_pool.initialize()

    async with db_pool.session_factory() as session:
        query = text(
            """
            SELECT
                property_id,
                COALESCE(SUM(total_amount), 0) AS total_revenue,
                COUNT(*) AS reservation_count
            FROM reservations
            WHERE property_id = :property_id AND tenant_id = :tenant_id
            GROUP BY property_id
            """
        )
        result = await session.execute(
            query, {"property_id": property_id, "tenant_id": tenant_id}
        )
        row = result.fetchone()

        if row:
            total_revenue = Decimal(str(row.total_revenue)).quantize(Decimal("0.01"))
            return {
                "property_id": property_id,
                "tenant_id": tenant_id,
                "total": str(total_revenue),
                "currency": "USD",
                "count": row.reservation_count,
            }

        return {
            "property_id": property_id,
            "tenant_id": tenant_id,
            "total": "0.00",
            "currency": "USD",
            "count": 0,
        }
