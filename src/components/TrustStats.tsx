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
    <>
      {/* Mobile: a single condensed strip, not three cards. Even the
          icon-on-top card version was ~150px of "nice to know, not the
          main event" content pushing the actual search form down the
          page -- collapsed to one line (icon + a short self-contained
          phrase per stat, dot-separated) instead. Card grid (sm: below)
          is display:none here, so nothing is announced twice to a
          screen reader -- only one of the two ever has a box. */}
      <div
        className="sm:hidden flex flex-wrap items-center justify-center gap-x-2.5 gap-y-1 mb-4 text-xs text-neutral-700"
        aria-label="Directory statistics"
      >
        {stats.map(({ icon: Icon, short }, i) => (
          <span key={short} className="inline-flex items-center gap-2.5">
            {i > 0 && <span aria-hidden="true" className="text-neutral-300">&bull;</span>}
            <span className="inline-flex items-center gap-1">
              <Icon className="w-3.5 h-3.5 text-primary-700 flex-shrink-0" aria-hidden="true" />
              {short}
            </span>
          </span>
        ))}
      </div>

      {/* sm and up: the fuller card treatment, unchanged -- there's
          enough width here that three cards read as real content, not
          clutter. Equal-width columns (not flex+justify-between) so the
          middle stat's container is genuinely centered regardless of how
          long the other two's text is. */}
      <div className="hidden sm:grid sm:grid-cols-3 gap-4 mb-6" aria-label="Directory statistics">
        {stats.map(({ icon: Icon, value, label }) => (
          <div
            key={label}
            className="flex items-center gap-3 bg-white border border-neutral-200 rounded-xl p-4 min-w-0"
          >
            <div className="w-11 h-11 md:w-12 md:h-12 bg-primary-100 rounded-full flex items-center justify-center flex-shrink-0">
              <Icon className="w-5 h-5 md:w-6 md:h-6 text-primary-700" aria-hidden="true" />
            </div>
            <div className="min-w-0">
              <p className="text-lg md:text-xl font-heading font-bold text-navy-800 leading-tight">{value}</p>
              <p className="text-xs md:text-sm text-neutral-600">{label}</p>
            </div>
          </div>
        ))}
      </div>
    </>
  );
}
