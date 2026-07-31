import { useState, useEffect } from 'react';
import { X, MapPin, AlertCircle, CheckCircle2 } from 'lucide-react';
import { supabase } from '../lib/supabase';
import {
  formatPhoneNumber,
  formatPostalCode,
  validateEmail,
  validatePhoneNumber,
  validatePostalCode,
  validateUrl,
  validateRequired,
  normalizeUrl,
  capitalizeWords,
} from '../lib/formValidation';
import type { Location, Organization, ServiceType } from '../types/database';

interface ListingFormProps {
  locationId?: string;
  onClose: () => void;
  onSave: () => void;
}

interface FormData {
  organization_id: string;
  organization_name: string;
  location_name: string;
  address_line_1: string;
  address_line_2: string;
  city: string;
  state: string;
  postal_code: string;
  county: string;
  latitude: string;
  longitude: string;
  public_phone: string;
  public_email: string;
  website_url: string;
  service_area_text: string;
  is_enhanced: boolean;
  insurance_accepted: string;
  featured_in_alphabetical: boolean;
  display_order: string;
  listing_status: string;
  service_type_ids: string[];
}

interface ValidationErrors {
  [key: string]: string;
}

const STATES = [
  'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
  'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
  'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
  'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
  'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
];

export default function ListingForm({ locationId, onClose, onSave }: ListingFormProps) {
  const [loading, setLoading] = useState(false);
  const [geocoding, setGeocoding] = useState(false);
  const [organizations, setOrganizations] = useState<Organization[]>([]);
  const [serviceTypes, setServiceTypes] = useState<ServiceType[]>([]);
  const [errors, setErrors] = useState<ValidationErrors>({});
  const [touched, setTouched] = useState<{ [key: string]: boolean }>({});
  const [showNewOrgForm, setShowNewOrgForm] = useState(false);

  const [formData, setFormData] = useState<FormData>({
    organization_id: '',
    organization_name: '',
    location_name: '',
    address_line_1: '',
    address_line_2: '',
    city: '',
    state: '',
    postal_code: '',
    county: '',
    latitude: '',
    longitude: '',
    public_phone: '',
    public_email: '',
    website_url: '',
    service_area_text: '',
    is_enhanced: false,
    insurance_accepted: '',
    featured_in_alphabetical: false,
    display_order: '999999',
    listing_status: 'draft',
    service_type_ids: [],
  });

  useEffect(() => {
    loadInitialData();
  }, [locationId]);

  async function loadInitialData() {
    try {
      const [orgsResponse, servicesResponse] = await Promise.all([
        supabase.from('organizations').select('*').order('organization_name'),
        supabase.from('service_types').select('*').order('display_order'),
      ]);

      if (orgsResponse.data) setOrganizations(orgsResponse.data);
      if (servicesResponse.data) setServiceTypes(servicesResponse.data);

      if (locationId) {
        const { data: location } = await supabase
          .from('locations')
          .select('*, location_service_types(service_type_id)')
          .eq('location_id', locationId)
          .single();

        if (location) {
          const serviceTypeIds =
            location.location_service_types?.map((lst: any) => lst.service_type_id) || [];

          setFormData({
            organization_id: location.organization_id,
            organization_name: '',
            location_name: location.location_name || '',
            address_line_1: location.address_line_1,
            address_line_2: location.address_line_2 || '',
            city: location.city,
            state: location.state,
            postal_code: location.postal_code,
            county: location.county || '',
            latitude: location.latitude?.toString() || '',
            longitude: location.longitude?.toString() || '',
            public_phone: location.public_phone || '',
            public_email: location.public_email || '',
            website_url: location.website_url || '',
            service_area_text: location.service_area_text || '',
            is_enhanced: (location as any).is_enhanced || false,
            insurance_accepted: ((location as any).insurance_accepted || []).join(', '),
            featured_in_alphabetical: (location as any).featured_in_alphabetical || false,
            display_order: ((location as any).display_order || 999999).toString(),
            listing_status: location.listing_status || 'draft',
            service_type_ids: serviceTypeIds,
          });
        }
      }
    } catch (error) {
      console.error('Error loading data:', error);
    }
  }

  function handleInputChange(field: keyof FormData, value: any) {
    setFormData((prev) => ({ ...prev, [field]: value }));
    setTouched((prev) => ({ ...prev, [field]: true }));

    validateField(field, value);
  }

  function handlePhoneChange(value: string) {
    const formatted = formatPhoneNumber(value);
    handleInputChange('public_phone', formatted);
  }

  function handlePostalCodeChange(value: string) {
    const formatted = formatPostalCode(value);
    handleInputChange('postal_code', formatted);
  }

  function validateField(field: keyof FormData, value: any) {
    const newErrors = { ...errors };

    switch (field) {
      case 'organization_id':
      case 'organization_name':
        if (!showNewOrgForm && !value) {
          newErrors[field] = 'Organization is required';
        } else {
          delete newErrors[field];
        }
        break;

      case 'address_line_1':
      case 'city':
      case 'state':
        if (!validateRequired(value)) {
          newErrors[field] = 'This field is required';
        } else {
          delete newErrors[field];
        }
        break;

      case 'postal_code':
        if (!validateRequired(value)) {
          newErrors[field] = 'This field is required';
        } else if (!validatePostalCode(value)) {
          newErrors[field] = 'Invalid postal code';
        } else {
          delete newErrors[field];
        }
        break;

      case 'public_phone':
        if (value && !validatePhoneNumber(value)) {
          newErrors[field] = 'Phone number must be 10 digits';
        } else {
          delete newErrors[field];
        }
        break;

      case 'public_email':
        if (value && !validateEmail(value)) {
          newErrors[field] = 'Invalid email address';
        } else {
          delete newErrors[field];
        }
        break;

      case 'website_url':
        if (value && !validateUrl(normalizeUrl(value))) {
          newErrors[field] = 'Invalid URL';
        } else {
          delete newErrors[field];
        }
        break;

      default:
        break;
    }

    setErrors(newErrors);
  }

  function validateForm(): boolean {
    const newErrors: ValidationErrors = {};

    if (!showNewOrgForm && !formData.organization_id) {
      newErrors.organization_id = 'Organization is required';
    }

    if (showNewOrgForm && !validateRequired(formData.organization_name)) {
      newErrors.organization_name = 'Organization name is required';
    }

    if (!validateRequired(formData.address_line_1)) {
      newErrors.address_line_1 = 'Address is required';
    }

    if (!validateRequired(formData.city)) {
      newErrors.city = 'City is required';
    }

    if (!validateRequired(formData.state)) {
      newErrors.state = 'State is required';
    }

    if (!validateRequired(formData.postal_code) || !validatePostalCode(formData.postal_code)) {
      newErrors.postal_code = 'Valid postal code is required';
    }

    if (formData.public_phone && !validatePhoneNumber(formData.public_phone)) {
      newErrors.public_phone = 'Phone number must be 10 digits';
    }

    if (formData.public_email && !validateEmail(formData.public_email)) {
      newErrors.public_email = 'Invalid email address';
    }

    if (formData.website_url && !validateUrl(normalizeUrl(formData.website_url))) {
      newErrors.website_url = 'Invalid URL';
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  }

  async function geocodeAddress() {
    const address = `${formData.address_line_1}, ${formData.city}, ${formData.state} ${formData.postal_code}`;

    setGeocoding(true);

    try {
      const response = await fetch(
        `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(address)}&key=${import.meta.env.VITE_GOOGLE_MAPS_API_KEY}`
      );
      const data = await response.json();

      if (data.status === 'OK' && data.results && data.results.length > 0) {
        const result = data.results[0];
        const location = result.geometry.location;

        setFormData((prev) => ({
          ...prev,
          latitude: location.lat.toString(),
          longitude: location.lng.toString(),
        }));

        return { lat: location.lat, lng: location.lng };
      } else {
        throw new Error('Geocoding failed');
      }
    } catch (error) {
      console.error('Geocoding error:', error);
      throw error;
    } finally {
      setGeocoding(false);
    }
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();

    if (!validateForm()) {
      return;
    }

    setLoading(true);

    try {
      let orgId = formData.organization_id;

      if (showNewOrgForm && formData.organization_name) {
        const orgSlug = formData.organization_name
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, '-')
          .replace(/^-|-$/g, '');

        const { data: newOrg, error: orgError } = await supabase
          .from('organizations')
          .insert({
            organization_name: formData.organization_name,
            organization_slug: orgSlug,
            website_url: formData.website_url ? normalizeUrl(formData.website_url) : null,
            main_phone: formData.public_phone || null,
          })
          .select()
          .single();

        if (orgError) throw orgError;
        orgId = newOrg.organization_id;
      }

      await geocodeAddress();

      const insuranceArray = formData.insurance_accepted
        ? formData.insurance_accepted.split(',').map((s) => s.trim()).filter(Boolean)
        : [];

      const locationData = {
        organization_id: orgId,
        location_name: formData.location_name || null,
        address_line_1: formData.address_line_1,
        address_line_2: formData.address_line_2 || null,
        city: capitalizeWords(formData.city),
        state: formData.state.toUpperCase(),
        postal_code: formData.postal_code,
        county: formData.county || null,
        latitude: formData.latitude ? parseFloat(formData.latitude) : null,
        longitude: formData.longitude ? parseFloat(formData.longitude) : null,
        public_phone: formData.public_phone || null,
        public_email: formData.public_email || null,
        website_url: formData.website_url ? normalizeUrl(formData.website_url) : null,
        service_area_text: formData.service_area_text || null,
        is_enhanced: formData.is_enhanced,
        insurance_accepted: insuranceArray.length > 0 ? insuranceArray : null,
        featured_in_alphabetical: formData.featured_in_alphabetical,
        display_order: parseInt(formData.display_order) || 999999,
        listing_status: formData.listing_status,
      };

      let savedLocationId = locationId;

      if (locationId) {
        const { error: updateError } = await supabase
          .from('locations')
          .update(locationData)
          .eq('location_id', locationId);

        if (updateError) throw updateError;

        await supabase
          .from('location_service_types')
          .delete()
          .eq('location_id', locationId);
      } else {
        const { data: newLocation, error: insertError } = await supabase
          .from('locations')
          .insert(locationData)
          .select()
          .single();

        if (insertError) throw insertError;
        savedLocationId = newLocation.location_id;
      }

      if (formData.service_type_ids.length > 0 && savedLocationId) {
        const serviceTypeRecords = formData.service_type_ids.map((serviceTypeId) => ({
          location_id: savedLocationId,
          service_type_id: serviceTypeId,
        }));

        const { error: serviceError } = await supabase
          .from('location_service_types')
          .insert(serviceTypeRecords);

        if (serviceError) throw serviceError;
      }

      onSave();
      onClose();
    } catch (error) {
      console.error('Error saving listing:', error);
      alert('Failed to save listing. Please try again.');
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4 overflow-y-auto">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-4xl my-8">
        <div className="flex items-center justify-between p-6 border-b border-neutral-200">
          <h2 className="text-2xl font-bold text-navy-800">
            {locationId ? 'Edit Listing' : 'Add New Listing'}
          </h2>
          <button
            onClick={onClose}
            className="text-neutral-400 hover:text-neutral-600 transition-colors"
          >
            <X className="w-6 h-6" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="p-6 space-y-6">
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
            <div className="flex items-start gap-3">
              <AlertCircle className="w-5 h-5 text-blue-600 flex-shrink-0 mt-0.5" />
              <div className="text-sm text-blue-800">
                <p className="font-semibold mb-1">Auto-Geocoding Enabled</p>
                <p>
                  When you save this listing, the address will be automatically geocoded to populate
                  latitude and longitude coordinates.
                </p>
              </div>
            </div>
          </div>

          <div>
            <label className="block text-sm font-semibold text-navy-800 mb-2">
              Organization
            </label>
            {!showNewOrgForm ? (
              <div className="space-y-2">
                <select
                  value={formData.organization_id}
                  onChange={(e) => handleInputChange('organization_id', e.target.value)}
                  className={`w-full px-4 py-2.5 border rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all ${
                    errors.organization_id && touched.organization_id
                      ? 'border-red-500'
                      : 'border-neutral-300'
                  }`}
                >
                  <option value="">Select an organization</option>
                  {organizations.map((org) => (
                    <option key={org.organization_id} value={org.organization_id}>
                      {org.organization_name}
                    </option>
                  ))}
                </select>
                <button
                  type="button"
                  onClick={() => setShowNewOrgForm(true)}
                  className="text-sm text-primary-600 hover:text-primary-700 font-medium"
                >
                  + Create New Organization
                </button>
                {errors.organization_id && touched.organization_id && (
                  <p className="text-sm text-red-600">{errors.organization_id}</p>
                )}
              </div>
            ) : (
              <div className="space-y-2">
                <input
                  type="text"
                  value={formData.organization_name}
                  onChange={(e) => handleInputChange('organization_name', e.target.value)}
                  placeholder="Enter organization name"
                  className={`w-full px-4 py-2.5 border rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all ${
                    errors.organization_name && touched.organization_name
                      ? 'border-red-500'
                      : 'border-neutral-300'
                  }`}
                />
                <button
                  type="button"
                  onClick={() => {
                    setShowNewOrgForm(false);
                    setFormData((prev) => ({ ...prev, organization_name: '' }));
                  }}
                  className="text-sm text-neutral-600 hover:text-neutral-700"
                >
                  Cancel - Select Existing
                </button>
                {errors.organization_name && touched.organization_name && (
                  <p className="text-sm text-red-600">{errors.organization_name}</p>
                )}
              </div>
            )}
          </div>

          <div>
            <label className="block text-sm font-semibold text-navy-800 mb-2">
              Location Name (Optional)
            </label>
            <input
              type="text"
              value={formData.location_name}
              onChange={(e) => handleInputChange('location_name', e.target.value)}
              placeholder="e.g., North Dallas Office"
              className="w-full px-4 py-2.5 border border-neutral-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all"
            />
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="md:col-span-2">
              <label className="block text-sm font-semibold text-navy-800 mb-2">
                Address Line 1 *
              </label>
              <input
                type="text"
                value={formData.address_line_1}
                onChange={(e) => handleInputChange('address_line_1', e.target.value)}
                placeholder="123 Main Street"
                className={`w-full px-4 py-2.5 border rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all ${
                  errors.address_line_1 && touched.address_line_1
                    ? 'border-red-500'
                    : 'border-neutral-300'
                }`}
              />
              {errors.address_line_1 && touched.address_line_1 && (
                <p className="text-sm text-red-600 mt-1">{errors.address_line_1}</p>
              )}
            </div>

            <div className="md:col-span-2">
              <label className="block text-sm font-semibold text-navy-800 mb-2">
                Address Line 2
              </label>
              <input
                type="text"
                value={formData.address_line_2}
                onChange={(e) => handleInputChange('address_line_2', e.target.value)}
                placeholder="Suite 100"
                className="w-full px-4 py-2.5 border border-neutral-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all"
              />
            </div>

            <div>
              <label className="block text-sm font-semibold text-navy-800 mb-2">City *</label>
              <input
                type="text"
                value={formData.city}
                onChange={(e) => handleInputChange('city', e.target.value)}
                placeholder="Dallas"
                className={`w-full px-4 py-2.5 border rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all ${
                  errors.city && touched.city ? 'border-red-500' : 'border-neutral-300'
                }`}
              />
              {errors.city && touched.city && (
                <p className="text-sm text-red-600 mt-1">{errors.city}</p>
              )}
            </div>

            <div>
              <label className="block text-sm font-semibold text-navy-800 mb-2">State *</label>
              <select
                value={formData.state}
                onChange={(e) => handleInputChange('state', e.target.value)}
                className={`w-full px-4 py-2.5 border rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all ${
                  errors.state && touched.state ? 'border-red-500' : 'border-neutral-300'
                }`}
              >
                <option value="">Select State</option>
                {STATES.map((state) => (
                  <option key={state} value={state}>
                    {state}
                  </option>
                ))}
              </select>
              {errors.state && touched.state && (
                <p className="text-sm text-red-600 mt-1">{errors.state}</p>
              )}
            </div>

            <div>
              <label className="block text-sm font-semibold text-navy-800 mb-2">
                Postal Code *
              </label>
              <input
                type="text"
                value={formData.postal_code}
                onChange={(e) => handlePostalCodeChange(e.target.value)}
                placeholder="75201"
                maxLength={10}
                className={`w-full px-4 py-2.5 border rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all ${
                  errors.postal_code && touched.postal_code
                    ? 'border-red-500'
                    : 'border-neutral-300'
                }`}
              />
              {errors.postal_code && touched.postal_code && (
                <p className="text-sm text-red-600 mt-1">{errors.postal_code}</p>
              )}
            </div>

            <div>
              <label className="block text-sm font-semibold text-navy-800 mb-2">County</label>
              <input
                type="text"
                value={formData.county}
                onChange={(e) => handleInputChange('county', e.target.value)}
                placeholder="Dallas County"
                className="w-full px-4 py-2.5 border border-neutral-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all"
              />
            </div>
          </div>

          <div className="bg-neutral-50 border border-neutral-200 rounded-lg p-4">
            <div className="flex items-center gap-2 mb-3">
              <MapPin className="w-5 h-5 text-primary-600" />
              <h3 className="font-semibold text-navy-800">Geocoding Coordinates</h3>
            </div>
            <p className="text-sm text-neutral-600 mb-3">
              These fields will auto-populate when you save the listing
            </p>
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-neutral-700 mb-1">
                  Latitude
                </label>
                <input
                  type="text"
                  value={formData.latitude}
                  readOnly
                  disabled
                  className="w-full px-4 py-2.5 bg-neutral-100 border border-neutral-300 rounded-lg text-neutral-600 cursor-not-allowed"
                  placeholder="Auto-populated"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-neutral-700 mb-1">
                  Longitude
                </label>
                <input
                  type="text"
                  value={formData.longitude}
                  readOnly
                  disabled
                  className="w-full px-4 py-2.5 bg-neutral-100 border border-neutral-300 rounded-lg text-neutral-600 cursor-not-allowed"
                  placeholder="Auto-populated"
                />
              </div>
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-semibold text-navy-800 mb-2">
                Phone Number
              </label>
              <input
                type="tel"
                value={formData.public_phone}
                onChange={(e) => handlePhoneChange(e.target.value)}
                placeholder="(555) 123-4567"
                maxLength={14}
                className={`w-full px-4 py-2.5 border rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all ${
                  errors.public_phone && touched.public_phone
                    ? 'border-red-500'
                    : 'border-neutral-300'
                }`}
              />
              {errors.public_phone && touched.public_phone && (
                <p className="text-sm text-red-600 mt-1">{errors.public_phone}</p>
              )}
            </div>

            <div>
              <label className="block text-sm font-semibold text-navy-800 mb-2">Email</label>
              <input
                type="email"
                value={formData.public_email}
                onChange={(e) => handleInputChange('public_email', e.target.value)}
                placeholder="contact@example.com"
                className={`w-full px-4 py-2.5 border rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all ${
                  errors.public_email && touched.public_email
                    ? 'border-red-500'
                    : 'border-neutral-300'
                }`}
              />
              {errors.public_email && touched.public_email && (
                <p className="text-sm text-red-600 mt-1">{errors.public_email}</p>
              )}
            </div>
          </div>

          <div>
            <label className="block text-sm font-semibold text-navy-800 mb-2">Website</label>
            <input
              type="text"
              value={formData.website_url}
              onChange={(e) => handleInputChange('website_url', e.target.value)}
              placeholder="www.example.com"
              className={`w-full px-4 py-2.5 border rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all ${
                errors.website_url && touched.website_url ? 'border-red-500' : 'border-neutral-300'
              }`}
            />
            {errors.website_url && touched.website_url && (
              <p className="text-sm text-red-600 mt-1">{errors.website_url}</p>
            )}
          </div>

          <div>
            <label className="block text-sm font-semibold text-navy-800 mb-2">
              Service Types
            </label>
            <div className="grid grid-cols-2 gap-3">
              {serviceTypes.map((serviceType) => (
                <label
                  key={serviceType.service_type_id}
                  className="flex items-center gap-2 p-3 border border-neutral-300 rounded-lg cursor-pointer hover:bg-neutral-50 transition-colors"
                >
                  <input
                    type="checkbox"
                    checked={formData.service_type_ids.includes(serviceType.service_type_id)}
                    onChange={(e) => {
                      const newIds = e.target.checked
                        ? [...formData.service_type_ids, serviceType.service_type_id]
                        : formData.service_type_ids.filter((id) => id !== serviceType.service_type_id);
                      handleInputChange('service_type_ids', newIds);
                    }}
                    className="w-4 h-4 text-primary-600 rounded focus:ring-2 focus:ring-primary-500"
                  />
                  <span className="text-sm text-neutral-700">{serviceType.service_type_name}</span>
                </label>
              ))}
            </div>
          </div>

          <div className="border-t border-neutral-200 pt-6">
            <div className="flex items-center gap-3 mb-4">
              <input
                type="checkbox"
                id="is_enhanced"
                checked={formData.is_enhanced}
                onChange={(e) => handleInputChange('is_enhanced', e.target.checked)}
                className="w-5 h-5 text-primary-600 rounded focus:ring-2 focus:ring-primary-500"
              />
              <label htmlFor="is_enhanced" className="text-sm font-semibold text-navy-800">
                Enhanced Listing (Premium)
              </label>
            </div>

            {formData.is_enhanced && (
              <div className="space-y-4 bg-amber-50 border border-amber-200 rounded-lg p-4">
                <p className="text-sm font-medium text-amber-900">
                  Enhanced Listing Features
                </p>

                <div>
                  <label className="block text-sm font-semibold text-navy-800 mb-2">
                    Insurance Accepted
                  </label>
                  <input
                    type="text"
                    value={formData.insurance_accepted}
                    onChange={(e) => handleInputChange('insurance_accepted', e.target.value)}
                    placeholder="Medicare, Medicaid, Blue Cross, Aetna"
                    className="w-full px-4 py-2.5 border border-neutral-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all"
                  />
                  <p className="text-xs text-neutral-500 mt-1">Separate multiple with commas</p>
                </div>

                <div>
                  <label className="block text-sm font-semibold text-navy-800 mb-2">
                    Service Area Description
                  </label>
                  <textarea
                    value={formData.service_area_text}
                    onChange={(e) => handleInputChange('service_area_text', e.target.value)}
                    placeholder="e.g., Serving Harris County and surrounding areas"
                    rows={3}
                    className="w-full px-4 py-2.5 border border-neutral-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all resize-none"
                  />
                </div>

                <div className="flex items-center gap-3">
                  <input
                    type="checkbox"
                    id="featured_in_alphabetical"
                    checked={formData.featured_in_alphabetical}
                    onChange={(e) => handleInputChange('featured_in_alphabetical', e.target.checked)}
                    className="w-4 h-4 text-primary-600 rounded focus:ring-2 focus:ring-primary-500"
                  />
                  <label htmlFor="featured_in_alphabetical" className="text-sm text-neutral-700">
                    Feature first in alphabetical order
                  </label>
                </div>

                <div>
                  <label className="block text-sm font-semibold text-navy-800 mb-2">
                    Display Order (Lower = Higher Priority)
                  </label>
                  <input
                    type="number"
                    value={formData.display_order}
                    onChange={(e) => handleInputChange('display_order', e.target.value)}
                    placeholder="999999"
                    className="w-full px-4 py-2.5 border border-neutral-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all"
                  />
                  <p className="text-xs text-neutral-500 mt-1">
                    Lower numbers appear first. Default: 999999
                  </p>
                </div>
              </div>
            )}
          </div>

          <div>
            <label className="block text-sm font-semibold text-navy-800 mb-2">
              Listing Status
            </label>
            <select
              value={formData.listing_status}
              onChange={(e) => handleInputChange('listing_status', e.target.value)}
              className="w-full px-4 py-2.5 border border-neutral-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all"
            >
              <option value="draft">Draft</option>
              <option value="needs_review">Needs Review</option>
              <option value="approved">Approved</option>
              <option value="published">Published</option>
              <option value="unpublished">Unpublished</option>
            </select>
          </div>

          <div className="flex items-center justify-between pt-6 border-t border-neutral-200">
            <button
              type="button"
              onClick={onClose}
              className="px-6 py-2.5 border border-neutral-300 text-neutral-700 rounded-lg hover:bg-neutral-50 transition-colors font-medium"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={loading || geocoding}
              className="px-6 py-2.5 bg-primary-600 text-white rounded-lg hover:bg-primary-700 transition-colors font-medium disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
            >
              {geocoding ? (
                <>
                  <MapPin className="w-4 h-4 animate-pulse" />
                  Geocoding...
                </>
              ) : loading ? (
                'Saving...'
              ) : (
                <>
                  <CheckCircle2 className="w-4 h-4" />
                  Save Listing
                </>
              )}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
