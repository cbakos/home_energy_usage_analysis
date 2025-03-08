import asyncio

from datetime import date
from pprint import pprint

from energyzero import EnergyZero, VatOption


async def main() -> None:
    """Show example on fetching the energy prices from EnergyZero."""
    async with EnergyZero(vat=VatOption.INCLUDE) as client:
        start_date = date(2025, 2, 15)
        end_date = date(2025, 2, 20)

        energy = await client.energy_prices(start_date, end_date)
        gas = await client.gas_prices(start_date, end_date)
        pprint(energy)
        pprint(gas)


if __name__ == "__main__":
    asyncio.run(main())