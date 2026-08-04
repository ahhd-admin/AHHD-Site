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
    },
    {
      icon: RefreshCw,
      value: 'Updated Daily',
      label: 'Fresh accreditation data every night',
    },
    {
      // Confirmed against live data (2026-08-02): published, MVP-scope
      // providers span 49 of 50 states plus DC and Puerto Rico -- a
      // real "nationwide" claim, not marketing language.
      icon: MapPinned,
      value: 'Nationwide Coverage',
      label: 'Accredited providers across the U.S.',
    },
  ];

  return (
    // grid-cols-3 at every width, not just sm+ -- three full stacked cards
    // on mobile pushed the actual search form noticeably further down the
    // page for what's a "nice to know, not the main event" trust signal.
    // A single condensed row (small icon, small text, tight padding) gets
    // the same three facts across in a fraction of the vertical space;
    // sm: restores the original larger, more spacious card sizing once
    // there's width to spare. Equal-width columns (not flex+justify-
    // between) so the middle stat's container is genuinely centered
    // regardless of how long the other two's text happens to be.
    <div className="grid grid-cols-3 gap-2 sm:gap-4 mb-4 md:mb-6" aria-label="Directory statistics">
      {stats.map(({ icon: Icon, value, label }) => (
        <div
          key={label}
          className="flex items-center gap-2 sm:gap-3 bg-white border border-neutral-200 rounded-xl px-2 py-2 sm:p-4 min-w-0"
        >
          <div className="w-7 h-7 sm:w-11 sm:h-11 md:w-12 md:h-12 bg-primary-100 rounded-full flex items-center justify-center flex-shrink-0">
            <Icon className="w-3.5 h-3.5 sm:w-5 sm:h-5 md:w-6 md:h-6 text-primary-700" aria-hidden="true" />
          </div>
          <div className="min-w-0">
            <p className="text-xs sm:text-lg md:text-xl font-heading font-bold text-navy-800 leading-tight">{value}</p>
            <p className="text-[10px] sm:text-xs md:text-sm text-neutral-600 leading-snug sm:leading-normal">{label}</p>
          </div>
        </div>
      ))}
    </div>
  );
}
