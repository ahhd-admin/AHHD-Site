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
    // grid-cols-3 (equal thirds), not flex+justify-between -- justify-between
    // positions the middle item based on the OTHER TWO items' own content
    // widths, not the container's true center, so the middle stat visibly
    // drifted off-center from the page whenever the first/third items'
    // text happened to differ in width. Equal-width columns guarantee the
    // middle one actually centers. Trades away the previous version's
    // edge-alignment to the search panel/map below (a subtler nuance the
    // uneven centering was more noticeable than) -- each stat now in its
    // own bordered container for real visual weight instead of floating
    // text, which also reads as more intentional on its own.
    <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-4 md:mb-6" aria-label="Directory statistics">
      {stats.map(({ icon: Icon, value, label }) => (
        <div
          key={label}
          className="flex items-center gap-3 bg-white border border-neutral-200 rounded-xl p-4"
        >
          <div className="w-11 h-11 md:w-12 md:h-12 bg-primary-100 rounded-full flex items-center justify-center flex-shrink-0">
            <Icon className="w-5 h-5 md:w-6 md:h-6 text-primary-600" aria-hidden="true" />
          </div>
          <div>
            <p className="text-lg md:text-xl font-heading font-bold text-navy-800 leading-tight">{value}</p>
            <p className="text-xs md:text-sm text-neutral-600">{label}</p>
          </div>
        </div>
      ))}
    </div>
  );
}
