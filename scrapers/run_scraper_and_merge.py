import asyncio
import sys
from achc_scraper import main as scraper_main

async def main():
    print("=" * 60)
    print("ACHC Scraper - Fast Pull Version")
    print("=" * 60)

    try:
        await scraper_main()
        print("\nData successfully written to Google Sheets!")
    except Exception as e:
        print(f"\nERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
