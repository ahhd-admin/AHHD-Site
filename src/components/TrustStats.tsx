import { useEffect, useState } from 'react';
import { Users, RefreshCw, MapPinned } from 'lucide-react';
import { supabase } from '../lib/supabase';

// Kept in sync with the site's actual MVP scope (Home Care, Home
// Health, Hospice) rather than hardcoded to a point-in-time number --
// the nightly scraper genuinely updates this now (see
// daily-scraper.yml), so a hardcoded figure would go stale within
// days. head:true means this is a count-only request, not a full row
// fetch.
async function fetchProviderCount(): Promise<number | null> {
  const { count } = await supabase
    .from('locations')
    .select('location_id, location_service_types!inner(service_types!inner(service_type_slug))', {
      count: 'exact',
      head: true,
    })
    .eq('listing_status', 'published')
    .eq('accepts_public_display', true)
    .in('location_service_types.service_types.service_type_slug', ['home-care', 'home-health-care', 'hospice']);

  return typeof count === 'number' ? count : null;
}

export default function TrustStats() {
  const [providerCount, setProviderCount] = useState<number | null>(null);

  useEffect(() => {
    let cancelled = false;
    fetchProviderCount().then((count) => {
      if (!cancelled) setProviderCount(count);
    });
    return () => {
      cancelled = true;
    };
  }, []);

  const stats = [
    {
      icon: Users,
      value: providerCount !== null ? providerCount.toLocaleString() : ' ',
      label: 'Accredited providers',
      // Short, self-contained phrase for the mobile strip -- the full
      // card version pairs a bare number with "Accredited providers" on
      // its own line underneath, but a strip has no second line, so the
      // number needs its context folded into one phrase instead.
      short: providerCount !== null ? `${providerCount.toLocaleString()} Providers` : 'Providers',
    },
    {
      icon: RefreshCw,
      value: 'Updated Daily',
      label: 'Fresh accreditation data every night',
      short: 'Updated Daily',
    },
    {
      // Confirmed against live data (2026-08-02): published, MVP-scope
      // providers span 49 of 50 states plus DC and Puerto Rico -- a
      // real "nationwide" claim, not marketing language.
      icon: MapPinned,
      value: 'Nationwide Coverage',
      label: 'Accredited providers across the U.S.',
      short: 'Nationwide',
    },
  ];

  return (
    // One condensed strip at every width, not three cards -- the card
    // treatment (icon-on-top, bold value, border/shadow) reads as a row
    // of buttons/CTAs, competing with the actual search form for
    // attention and implying an action that tapping them doesn't
    // actually perform. A plain dot-separated line (icon + a short
    // self-contained phrase per stat) states these facts without
    // pretending to be interactive.
    <div
      // Left-aligned, not centered -- matches the heading above it. Below
      // lg, that's also the map's own left edge, since SearchHero stacks
      // the search panel above a full-width map there (see SearchHero.tsx)
      // -- no extra offset needed. At lg+, SearchHero switches to a
      // side-by-side row: a 300px search panel (lg:w-[300px]) plus a
      // 16px gap (gap-4) sits to the left of the map there, so aligning
      // to the container's own edge would land this above the search
      // panel instead, not the map -- reading as misaligned/a mistake
      // against the two-column grid beneath it once that gap exists. The
      // lg:pl-[316px] (300px + 16px) pushes it over to start exactly
      // where the map column starts once that offset exists, matching
      // SearchHero's own panel width + gap values.
      className="flex flex-wrap items-center justify-start gap-x-2.5 gap-y-1 mb-4 lg:pl-[316px] text-xs sm:text-sm text-neutral-700"
      aria-label="Directory statistics"
    >
      {stats.map(({ icon: Icon, short }, i) => (
        <span key={short} className="inline-flex items-center gap-2.5">
          {i > 0 && <span aria-hidden="true" className="text-neutral-300">&bull;</span>}
          <span className="inline-flex items-center gap-1">
            <Icon className="w-3.5 h-3.5 sm:w-4 sm:h-4 text-primary-700 flex-shrink-0" aria-hidden="true" />
            {short}
          </span>
        </span>
      ))}
    </div>
  );
}
