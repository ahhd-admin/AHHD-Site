import { MapPin, Phone, Globe, Shield, CheckCircle, Award, Star } from 'lucide-react';
import type { LocationWithDetails } from '../types/database';
import { formatDistance } from '../lib/geoUtils';
import { buildProviderSlug } from '../lib/slug';

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

  return (
    <a
      href={`/provider/${buildProviderSlug(location.organization?.organization_name || location.location_name || '', location.achc_source_id)}`}
      className={`card p-6 hover:shadow-xl transition-all duration-300 hover:-translate-y-1 block group relative ${isEnhanced ? 'ring-2 ring-amber-400' : ''}`}
    >
      {isEnhanced && (
        <div className="absolute -top-3 -right-3 bg-gradient-to-br from-amber-400 to-amber-500 text-neutral-900 px-3 py-1.5 rounded-full flex items-center gap-1.5 text-xs font-bold shadow-lg z-10">
          <Star className="w-3.5 h-3.5 fill-current" />
          <span>Enhanced</span>
        </div>
      )}

      <div className="flex flex-col h-full">
        <div className="flex items-start justify-between mb-4">
          <div className="flex-1">
            <h3 className="text-xl font-heading font-semibold text-navy-800 mb-2 group-hover:text-primary-600 transition-colors">
              {location.organization?.organization_name || 'Healthcare Provider'}
            </h3>
            {location.location_name && (
              <p className="text-sm text-neutral-600 mb-2">{location.location_name}</p>
            )}
          </div>

          {activeAccreditations.length > 0 && (
            <div className="flex-shrink-0 ml-4">
              {/* success-100/success-900 measures 8.57:1 (AAA); a light,
                  self-contained pill reads clearly on both this white card
                  and the dark navy provider-page banner (see
                  ProviderDetailPage.tsx), so both use the same pair. */}
              <div className="bg-success-100 text-success-900 px-3 py-1.5 rounded-full flex items-center gap-1.5 text-xs font-semibold">
                <Shield className="w-3.5 h-3.5" />
                <span>Accredited</span>
              </div>
            </div>
          )}
        </div>

        <div className="space-y-3 mb-4 flex-1">
          <div className="flex items-start gap-2 text-neutral-700">
            <CheckCircle className="w-5 h-5 text-primary-600 flex-shrink-0 mt-0.5" />
            <span className="text-sm font-medium">{serviceNames}</span>
          </div>

          <div className="flex items-start gap-2 text-neutral-700">
            <MapPin className="w-5 h-5 text-neutral-500 flex-shrink-0 mt-0.5" />
            <span className="text-sm">
              {location.city}, {location.state} {location.postal_code}
              {actualDistance && <span className="ml-2 text-primary-600 font-semibold">({formatDistance(actualDistance)})</span>}
            </span>
          </div>

          {location.public_phone && (
            <div className="flex items-center gap-2 text-neutral-700">
              <Phone className="w-5 h-5 text-neutral-500 flex-shrink-0" />
              <span className="text-sm text-neutral-700">
                {location.public_phone}
              </span>
            </div>
          )}

          {(location.website_url || location.organization?.website_url) && (
            <div className="flex items-center gap-2 text-neutral-700">
              <Globe className="w-5 h-5 text-neutral-500 flex-shrink-0" />
              <span className="text-sm text-primary-600 group-hover:text-primary-700 font-medium truncate">
                Visit Website
              </span>
            </div>
          )}
        </div>

        {activeAccreditations.length > 0 && (
          <div className="pt-4 border-t border-neutral-200">
            <div className="flex items-center gap-2 mb-2">
              <Award className="w-4 h-4 text-neutral-500" />
              <span className="text-xs font-semibold text-neutral-600 uppercase tracking-wide">
                Accredited By
              </span>
            </div>
            <div className="flex flex-wrap gap-2">
              {activeAccreditations.map((accreditation) => (
                <span
                  key={accreditation.accreditation_id}
                  className="inline-block bg-neutral-100 text-neutral-800 px-3 py-1 rounded-md text-xs font-medium border border-neutral-200"
                >
                  {accreditation.accrediting_body}
                </span>
              ))}
            </div>
          </div>
        )}

        <div className="mt-4 pt-4 border-t border-neutral-200">
          <span className="text-primary-600 group-hover:text-primary-700 font-semibold text-sm flex items-center justify-between">
            View Full Details
            <span aria-hidden="true" className="transform group-hover:translate-x-1 transition-transform">→</span>
          </span>
        </div>
      </div>
    </a>
  );
}
