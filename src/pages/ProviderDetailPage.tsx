import { useEffect, useState } from 'react';
import { MapPin, Phone, Globe, Mail, Shield, Calendar, CheckCircle, Award, Star, ExternalLink, Search } from 'lucide-react';
import Header from '../components/Header';
import Footer from '../components/Footer';
import { supabase } from '../lib/supabase';
import type { LocationWithDetails } from '../types/database';
import { parseAchcSourceIdFromSlug } from '../lib/slug';
import { getVerifiedDate, formatVerifiedDate } from '../lib/formatVerifiedDate';

interface ProviderDetailPageProps {
  locationId?: string;
}

export default function ProviderDetailPage({ locationId }: ProviderDetailPageProps) {
  const [location, setLocation] = useState<LocationWithDetails | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const slug = locationId || window.location.pathname.split('/').pop() || '';
    const achcSourceId = parseAchcSourceIdFromSlug(slug);
    if (achcSourceId) {
      loadProvider(achcSourceId);
    } else if (slug) {
      // Fallback for old bare-UUID links (e.g. bookmarked before the slug
      // format changed) -- treat the whole segment as a location_id.
      loadProvider(slug, true);
    }
  }, [locationId]);

  const loadProvider = async (id: string, byLocationId = false) => {
    setLoading(true);

    const { data, error } = await supabase
      .from('locations')
      .select(`
        *,
        organization:organizations(*),
        service_types:location_service_types(
          service_type:service_types(*)
        ),
        accreditation_records(*),
        listing_settings:location_listing_settings(*)
      `)
      .eq(byLocationId ? 'location_id' : 'achc_source_id', id)
      .maybeSingle();

    if (data && !error) {
      const mapped = {
        ...data,
        service_types: data.service_types?.map((st: any) => st.service_type).filter(Boolean) || [],
        listing_settings: Array.isArray(data.listing_settings) && data.listing_settings.length > 0
          ? data.listing_settings[0]
          : data.listing_settings
      };
      setLocation(mapped);
    }

    setLoading(false);
  };

  if (loading) {
    return (
      <div className="min-h-screen flex flex-col">
        <Header />
        <main className="flex-1 flex items-center justify-center py-20">
          <div className="text-center" role="status">
            <div className="inline-block animate-spin rounded-full h-12 w-12 border-4 border-primary-200 border-t-primary-600 mb-4" aria-hidden="true"></div>
            <p className="text-neutral-600">Loading provider details...</p>
          </div>
        </main>
        <Footer />
      </div>
    );
  }

  if (!location) {
    return (
      <div className="min-h-screen flex flex-col">
        <Header />
        <main className="flex-1 flex items-center justify-center py-20">
          <div className="text-center max-w-md">
            <h1 className="text-2xl font-heading font-semibold text-navy-800 mb-4">
              Provider Not Found
            </h1>
            <p className="text-neutral-700 mb-6">
              The provider you're looking for could not be found.
            </p>
            <a href="/find-care" className="btn-primary">
              Search for Providers
            </a>
          </div>
        </main>
        <Footer />
      </div>
    );
  }

  const activeAccreditations = location.accreditation_records?.filter(
    (record) => record.accreditation_status === 'active' && record.is_current_record
  ) || [];

  const isEnhanced = location.listing_settings?.is_featured || false;
  const enhancedDescription = location.listing_settings?.enhanced_description;
  const specialties = location.listing_settings?.specialties_text;
  const callToActionText = location.listing_settings?.call_to_action_text;
  const callToActionUrl = location.listing_settings?.call_to_action_url;

  return (
    <div className="min-h-screen flex flex-col">
      <Header />

      <main className="flex-1">
        <div className="bg-navy-700 text-white py-12 relative">
          {isEnhanced && (
            <div className="absolute top-6 right-8 bg-gradient-to-br from-amber-400 to-amber-500 text-neutral-900 px-4 py-2 rounded-full flex items-center gap-2 text-sm font-bold shadow-lg">
              <Star className="w-4 h-4 fill-current" />
              <span>Enhanced Listing</span>
            </div>
          )}

          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            {/* primary-200 on navy-700 measures 7.45:1 (AAA). This has to be
                set directly on each <a>, not just the wrapping div -- a
                global `a { text-primary-600 }` rule in index.css sets an
                explicit color on every anchor, and an explicit rule on the
                element itself always wins over an inherited one, no matter
                which layer it's in. That left "Home"/"Find Care" rendering
                primary-600 (3.05:1 on navy-700 -- fails AA) despite the
                parent div being styled correctly. */}
            <div className="flex items-center gap-2 text-primary-200 mb-4">
              <a href="/" className="text-primary-200 hover:text-white">Home</a>
              <span>/</span>
              <a href="/find-care" className="text-primary-200 hover:text-white">Find Care</a>
              <span>/</span>
              <span className="text-white">Provider Details</span>
            </div>

            <div className="flex items-start justify-between flex-wrap gap-4">
              <div className="flex-1 min-w-0">
                <h1 className="text-3xl md:text-4xl font-heading font-bold mb-3 text-white">
                  {location.organization?.organization_name || 'Healthcare Provider'}
                </h1>
                {/* location_name is currently a straight copy of the
                    organization name for essentially every record (real
                    DBA names haven't been extracted yet -- see
                    MVP-Rollout-Roadmap.md) -- only show it once it's
                    genuinely a different name. */}
                {location.location_name && location.location_name !== location.organization?.organization_name && (
                  <p className="text-xl text-primary-200 mb-4">{location.location_name}</p>
                )}
              </div>

              {activeAccreditations.length > 0 && (
                // flex-wrap on the parent (not a fixed ml-6) lets this
                // pill drop below the title on narrow screens instead of
                // getting pushed past the viewport edge -- confirmed via
                // a functional audit that the un-wrapped version caused a
                // page-wide horizontal scrollbar at 390px on every
                // provider page with an active accreditation (most of
                // them).
                <div className="flex-shrink-0">
                  {/* success-100/success-900 measures 8.57:1 (AAA) -- a
                      light, self-contained pill so the same badge style
                      reads clearly here (dark navy banner) and on the white
                      ProviderCard, rather than two different treatments.
                      (Earlier version was solid success-500/white text,
                      which measured 3.99:1 -- below the 4.5:1 AA floor.) */}
                  <div className="bg-success-100 text-success-900 px-4 py-2 rounded-lg flex items-center gap-2">
                    <Shield className="w-5 h-5" />
                    <span className="font-medium">Accredited</span>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>

        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
            <div className="lg:col-span-2 space-y-8">
              {isEnhanced && enhancedDescription && (
                <section className="card p-8 bg-gradient-to-br from-amber-50 to-amber-100 border-2 border-amber-200">
                  <div className="flex items-center gap-3 mb-3">
                    <Star className="w-6 h-6 text-amber-600 fill-current flex-shrink-0" />
                    <h2 className="text-2xl font-heading font-semibold text-navy-800">
                      About This Provider
                    </h2>
                  </div>
                  <p className="text-neutral-800 leading-relaxed text-base">
                    {enhancedDescription}
                  </p>
                </section>
              )}

              {isEnhanced && specialties && (
                <section className="card p-8">
                  <h2 className="text-2xl font-heading font-semibold text-navy-800 mb-6">
                    Areas of Expertise
                  </h2>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                    {specialties.split(',').map((specialty, index) => (
                      <div key={index} className="flex items-center gap-3 bg-neutral-50 px-4 py-3 rounded-lg">
                        <CheckCircle className="w-5 h-5 text-primary-600 flex-shrink-0" />
                        <span className="font-medium text-navy-800">{specialty.trim()}</span>
                      </div>
                    ))}
                  </div>
                </section>
              )}

              <section className="card p-8">
                <h2 className="text-2xl font-heading font-semibold text-navy-800 mb-6">
                  Services Offered
                </h2>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {location.service_types?.map((service) => (
                    <div key={service.service_type_id} className="flex items-start gap-3">
                      <CheckCircle className="w-6 h-6 text-primary-600 flex-shrink-0 mt-0.5" />
                      <div>
                        <p className="font-medium text-navy-800">{service.service_type_name}</p>
                        {service.description && (
                          <p className="text-sm text-neutral-600 mt-1">{service.description}</p>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </section>

              {activeAccreditations.length > 0 && (
                <section className="card p-8">
                  <h2 className="text-2xl font-heading font-semibold text-navy-800 mb-6 flex items-center gap-2">
                    <Award className="w-6 h-6 text-primary-600" />
                    Accreditation Details
                  </h2>
                  <div className="space-y-4">
                    {activeAccreditations.map((accreditation) => (
                      <div
                        key={accreditation.accreditation_id}
                        className="bg-neutral-50 rounded-lg p-6 border border-neutral-200"
                      >
                        <div className="flex items-start justify-between mb-3">
                          <div>
                            <p className="font-semibold text-navy-800 text-lg">
                              {accreditation.accrediting_body}
                            </p>
                            {/* Every accreditation_scope value currently in the DB is the
                                literal string "National raw ACHC pull; no state selected" --
                                an internal ingestion note, not real scope content, that was
                                rendering verbatim on every provider page right under the
                                credential meant to build trust. Filtered out by pattern
                                (not just hidden entirely) so real scope text starts showing
                                automatically once the scraper populates it. */}
                            {accreditation.accreditation_scope &&
                              !/raw ACHC pull/i.test(accreditation.accreditation_scope) && (
                                <p className="text-sm text-neutral-600 mt-1">
                                  {accreditation.accreditation_scope}
                                </p>
                              )}
                          </div>
                          <span className="badge-success">
                            {accreditation.accreditation_status}
                          </span>
                        </div>

                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mt-4">
                          {accreditation.effective_date && (
                            <div className="flex items-center gap-2 text-sm text-neutral-700">
                              <Calendar className="w-4 h-4 text-neutral-500" />
                              <span>
                                Effective: {new Date(accreditation.effective_date).toLocaleDateString()}
                              </span>
                            </div>
                          )}
                          {accreditation.expiration_date && (
                            <div className="flex items-center gap-2 text-sm text-neutral-700">
                              <Calendar className="w-4 h-4 text-neutral-500" />
                              <span>
                                Expires: {new Date(accreditation.expiration_date).toLocaleDateString()}
                              </span>
                            </div>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                </section>
              )}

              {location.service_area_text && (
                <section className="card p-8">
                  <h2 className="text-2xl font-heading font-semibold text-navy-800 mb-4">
                    Service Area
                  </h2>
                  <p className="text-neutral-700 leading-relaxed">{location.service_area_text}</p>
                </section>
              )}

              {location.is_sponsored && (
                <>
                  {location.insurance_accepted && location.insurance_accepted.length > 0 && (
                    <section className="card p-8">
                      <h2 className="text-2xl font-heading font-semibold text-navy-800 mb-6">
                        Insurance Accepted
                      </h2>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                        {location.insurance_accepted.map((insurance, index) => (
                          <div key={index} className="flex items-center gap-3 bg-neutral-50 px-4 py-3 rounded-lg">
                            <CheckCircle className="w-5 h-5 text-success-600 flex-shrink-0" />
                            <span className="font-medium text-navy-800">{insurance}</span>
                          </div>
                        ))}
                      </div>
                    </section>
                  )}

                  {location.service_area_description && (
                    <section className="card p-8">
                      <h2 className="text-2xl font-heading font-semibold text-navy-800 mb-4">
                        Service Area
                      </h2>
                      <p className="text-neutral-700 leading-relaxed">{location.service_area_description}</p>
                    </section>
                  )}

                  <section className="card p-8">
                    <h2 className="text-2xl font-heading font-semibold text-navy-800 mb-6">
                      Provider Details
                    </h2>
                    <div className="space-y-4">
                      {location.years_in_business && (
                        <div className="flex items-center gap-3">
                          <Award className="w-5 h-5 text-primary-600 flex-shrink-0" />
                          <span className="text-neutral-700">
                            <span className="font-semibold text-navy-800">{location.years_in_business}</span> years in business
                          </span>
                        </div>
                      )}

                      {location.availability_24_7 && (
                        <div className="flex items-center gap-3">
                          <CheckCircle className="w-5 h-5 text-success-600 flex-shrink-0" />
                          <span className="font-medium text-navy-800">24/7 availability</span>
                        </div>
                      )}

                      {location.languages_spoken && location.languages_spoken.length > 0 && (
                        <div>
                          <div className="flex items-start gap-3 mb-2">
                            <Globe className="w-5 h-5 text-primary-600 flex-shrink-0 mt-0.5" />
                            <span className="font-semibold text-navy-800">Languages Spoken:</span>
                          </div>
                          <div className="ml-8 flex flex-wrap gap-2">
                            {location.languages_spoken.map((language, index) => (
                              <span key={index} className="px-3 py-1 bg-neutral-100 text-neutral-800 rounded-full text-sm font-medium">
                                {language}
                              </span>
                            ))}
                          </div>
                        </div>
                      )}

                      {location.special_programs && location.special_programs.length > 0 && (
                        <div>
                          <div className="flex items-start gap-3 mb-2">
                            <Star className="w-5 h-5 text-primary-600 flex-shrink-0 mt-0.5" />
                            <span className="font-semibold text-navy-800">Special Programs:</span>
                          </div>
                          <div className="ml-8 space-y-2">
                            {location.special_programs.map((program, index) => (
                              <div key={index} className="flex items-center gap-2">
                                <CheckCircle className="w-4 h-4 text-primary-600 flex-shrink-0" />
                                <span className="text-neutral-700">{program}</span>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  </section>
                </>
              )}
            </div>

            <div className="space-y-6">
              {isEnhanced && callToActionText && callToActionUrl && (
                <div className="card p-6 bg-gradient-to-br from-amber-400 to-amber-500 text-neutral-900 border-none">
                  <h3 className="text-lg font-heading font-bold mb-3">
                    Ready to Get Started?
                  </h3>
                  <p className="text-sm mb-4 text-neutral-900 font-medium">
                    Contact us today to learn more about our services
                  </p>
                  <a
                    href={callToActionUrl}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="btn-secondary bg-white text-neutral-900 hover:bg-neutral-100 w-full flex items-center justify-center gap-2 font-bold shadow-md"
                  >
                    {callToActionText}
                    <ExternalLink className="w-4 h-4" />
                  </a>
                </div>
              )}

              <div className="card p-6 sticky top-24">
                <h3 className="text-xl font-heading font-semibold text-navy-800 mb-6">
                  Contact Information
                </h3>

                <div className="space-y-4">
                  <div className="flex items-start gap-3">
                    <MapPin className="w-5 h-5 text-neutral-500 flex-shrink-0 mt-1" />
                    <div className="text-neutral-700">
                      <p>{location.address_line_1}</p>
                      {location.address_line_2 && <p>{location.address_line_2}</p>}
                      <p>
                        {location.city}, {location.state} {location.postal_code}
                      </p>
                    </div>
                  </div>

                  {location.public_phone && (
                    <div className="flex items-center gap-3">
                      <Phone className="w-5 h-5 text-neutral-500 flex-shrink-0" />
                      <a
                        href={`tel:${location.public_phone}`}
                        className="text-primary-600 hover:text-primary-700 font-medium"
                      >
                        {location.public_phone}
                      </a>
                    </div>
                  )}

                  {location.public_email && (
                    <div className="flex items-center gap-3">
                      <Mail className="w-5 h-5 text-neutral-500 flex-shrink-0" />
                      <a
                        href={`mailto:${location.public_email}`}
                        className="text-primary-600 hover:text-primary-700 font-medium break-all"
                      >
                        {location.public_email}
                      </a>
                    </div>
                  )}

                  {(location.website_url || location.organization?.website_url) && (
                    <div className="flex items-center gap-3">
                      <Globe className="w-5 h-5 text-neutral-500 flex-shrink-0" />
                      <a
                        href={location.website_url || location.organization?.website_url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-primary-600 hover:text-primary-700 font-medium break-all"
                      >
                        Visit Website
                      </a>
                    </div>
                  )}
                </div>

                {/* Was reading last_verified_at directly, which is only ever
                    set once at import and goes stale -- confirmed live the
                    same listing showed "Last verified: yesterday" on its
                    search-results card (which already used this shared
                    helper) but a date months old here, contradicting
                    itself between the two screens. Now sourced the same
                    way everywhere: source_last_seen_at (the nightly
                    scraper's actual last-confirmed-at-source run) first. */}
                {formatVerifiedDate(getVerifiedDate(location)) && (
                  <div className="mt-6 pt-6 border-t border-neutral-200">
                    <p className="text-sm text-neutral-600">
                      {formatVerifiedDate(getVerifiedDate(location))}
                    </p>
                  </div>
                )}
              </div>

              <div className="card p-6 bg-primary-50 border-primary-200">
                <h3 className="font-heading font-semibold text-navy-800 mb-3">
                  About This Directory
                </h3>
                <p className="text-sm text-neutral-700 leading-relaxed mb-4">
                  This directory is informational only. AHHD does not endorse specific providers. Accreditation status can change, so please verify details directly with the provider.
                </p>
                {(() => {
                  const orgName = location.organization?.organization_name || '';
                  const addr = `${location.address_line_1 || ''} ${location.city || ''}, ${location.state || ''} ${location.postal_code || ''}`.trim();
                  const searchQuery = encodeURIComponent(`${orgName} ${addr}`.trim());
                  const searchUrl = `https://www.google.com/search?q=${searchQuery}`;
                  return (
                    <>
                      <a
                        href={searchUrl}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="btn-outline text-sm w-full flex items-center justify-center gap-2 mb-3"
                      >
                        <Search className="w-4 h-4" />
                        Search for this provider's website
                      </a>
                      <p className="text-xs text-neutral-600 leading-relaxed">
                        This search is launched using the information we have on file for this provider. Please confirm the results you see match before relying on them. Details can change over time, and some providers operate under a different name (a "doing business as" name) than the legal entity name on file with their accrediting body.
                      </p>
                    </>
                  );
                })()}
              </div>
            </div>
          </div>

          <div className="mt-12 text-center">
            {/* Was a plain <a href="/find-care"> -- a real page load to a
                bare URL with no query string, discarding whatever search
                (location, filters, radius, view) got the visitor here in
                the first place. Confirmed live: the browser's own Back
                button already restores everything correctly (search state
                round-trips through the URL query string), so this now
                just does the same thing instead of a fixed, param-less
                destination. Falls back to /find-care directly if there's
                nothing in history to go back to (e.g. a shared/bookmarked
                link straight to this page). */}
            <button
              onClick={() => {
                if (window.history.length > 1) {
                  window.history.back();
                } else {
                  window.location.href = '/find-care';
                }
              }}
              className="btn-outline"
            >
              ← Back to Search Results
            </button>
          </div>
        </div>
      </main>

      <Footer />
    </div>
  );
}
