import { MapPin, Phone, Globe, Shield, CheckCircle, Award, Star } from 'lucide-react';
import type { LocationWithDetails } from '../types/database';
import { formatDistance } from '../lib/geoUtils';
import { buildProviderSlug } from '../lib/slug';
import { saveHomeScrollPosition } from '../lib/scrollRestoration';

interface ProviderCardProps {
  location: LocationWithDetails & { distance?: number | null };
  distance?: number | null;
}

export default function ProviderCard({ location, distance }: ProviderCardProps) {
  const actualDistance = (location as any).distance ?? distance;
  const activeAccreditations = location.accreditation_records?.filter(
    (record) => record.accreditation_status === 'active' && record.is_current_record
  ) || [];

  const serviceNames = location.service_types?.map((st) => st.service_type_name).join(', ') || 'Care Services';
  const isEnhanced = location.listing_settings?.is_featured || false;

  // location_name is currently populated as a straight copy of the
  // organization name for essentially every record (real DBA/"doing
  // business as" names haven't been extracted yet -- see
  // MVP-Rollout-Roadmap.md), so showing both lines just repeats the same
  // text. Only show it once it's genuinely a different name.
  const hasDistinctLocationName =
    location.location_name && location.location_name !== location.organization?.organization_name;

  return (
    <a
      href={`/provider/${buildProviderSlug(location.organization?.organization_name || location.location_name || '', location.achc_source_id)}`}
      onClick={saveHomeScrollPosition}
      className={`card p-4 hover:shadow-xl transition-all duration-300 hover:-translate-y-1 block group relative ${isEnhanced ? 'ring-2 ring-amber-400' : ''}`}
    >
      {isEnhanced && (
        <div className="absolute -top-2.5 -right-2.5 bg-gradient-to-br from-amber-400 to-amber-500 text-neutral-900 px-2.5 py-1 rounded-full flex items-center gap-1.5 text-xs font-bold shadow-lg z-10">
          <Star className="w-3 h-3 fill-current" />
          <span>Enhanced</span>
        </div>
      )}

      <div className="flex flex-col h-full">
        <div className="flex items-start justify-between gap-2 mb-2.5">
          <div className="flex-1 min-w-0">
            <h3 className="text-base font-heading font-semibold text-navy-800 group-hover:text-primary-600 transition-colors leading-snug">
              {location.organization?.organization_name || 'Healthcare Provider'}
            </h3>
            {hasDistinctLocationName && (
              <p className="text-xs text-neutral-600 mt-0.5">{location.location_name}</p>
            )}
          </div>

          {activeAccreditations.length > 0 && (
            <div className="flex-shrink-0">
              {/* success-100/success-900 measures 8.57:1 (AAA); a light,
                  self-contained pill reads clearly on both this white card
                  and the dark navy provider-page banner (see
                  ProviderDetailPage.tsx), so both use the same pair. */}
              <div className="bg-success-100 text-success-900 px-2 py-1 rounded-full flex items-center gap-1 text-xs font-semibold whitespace-nowrap">
                <Shield className="w-3 h-3" />
                <span>Accredited</span>
              </div>
            </div>
          )}
        </div>

        <div className="space-y-1.5 mb-2.5 flex-1">
          <div className="flex items-start gap-1.5 text-neutral-700">
            <CheckCircle className="w-4 h-4 text-primary-600 flex-shrink-0 mt-0.5" />
            <span className="text-sm font-medium">{serviceNames}</span>
          </div>

          <div className="flex items-start gap-1.5 text-neutral-700">
            <MapPin className="w-4 h-4 text-neutral-500 flex-shrink-0 mt-0.5" />
            <span className="text-sm">
              {location.city}, {location.state} {location.postal_code}
              {actualDistance && <span className="ml-2 text-primary-600 font-semibold">({formatDistance(actualDistance)})</span>}
            </span>
          </div>

          {location.public_phone && (
            <div className="flex items-center gap-1.5 text-neutral-700">
              <Phone className="w-4 h-4 text-neutral-500 flex-shrink-0" />
              <span className="text-sm text-neutral-700">
                {location.public_phone}
              </span>
            </div>
          )}

          {(location.website_url || location.organization?.website_url) && (
            <div className="flex items-center gap-1.5 text-neutral-700">
              <Globe className="w-4 h-4 text-neutral-500 flex-shrink-0" />
              <span className="text-sm text-primary-600 group-hover:text-primary-700 font-medium truncate">
                Visit Website
              </span>
            </div>
          )}
        </div>

        {activeAccreditations.length > 0 && (
          <div className="pt-2.5 border-t border-neutral-200">
            <div className="flex items-center gap-1.5 mb-1.5">
              <Award className="w-3.5 h-3.5 text-neutral-500" />
              <span className="text-xs font-semibold text-neutral-600 uppercase tracking-wide">
                Accredited By
              </span>
            </div>
            <div className="flex flex-wrap gap-1.5">
              {activeAccreditations.map((accreditation) => (
                <span
                  key={accreditation.accreditation_id}
                  className="inline-block bg-neutral-100 text-neutral-800 px-2 py-0.5 rounded-md text-xs font-medium border border-neutral-200"
                >
                  {accreditation.accrediting_body}
                </span>
              ))}
            </div>
          </div>
        )}

        <div className="mt-2.5 pt-2.5 border-t border-neutral-200">
          <span className="text-primary-600 group-hover:text-primary-700 font-semibold text-sm flex items-center justify-between">
            View Full Details
            <span aria-hidden="true" className="transform group-hover:translate-x-1 transition-transform">→</span>
          </span>
        </div>
      </div>
    </a>
  );
}
