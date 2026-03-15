import asyncio
import sys
from achc_scraper import main as scraper_main

async def main():
    print("=" * 60)
    print("ACHC Scraper - Raw Dump Test")
    print("=" * 60)

    try:
        await scraper_main()
    except Exception as e:
        print(f"\nERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
