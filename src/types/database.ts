export type ListingStatus =
  | 'draft'
  | 'imported_raw'
  | 'normalized'
  | 'validated'
  | 'needs_review'
  | 'approved'
  | 'published'
  | 'unpublished'
  | 'expired'
  | 'rejected';

export type AccreditationStatus =
  | 'active'
  | 'expired'
  | 'pending'
  | 'revoked'
  | 'unknown';

export type ImagePurpose =
  | 'hero'
  | 'thumbnail'
  | 'logo'
  | 'content'
  | 'provider_logo'
  | 'background'
  | 'icon';

export type ContentStatus =
  | 'draft'
  | 'in_review'
  | 'published'
  | 'archived';

export type ReviewStatus =
  | 'open'
  | 'in_progress'
  | 'waiting_for_information'
  | 'resolved'
  | 'rejected';

export type ReviewType =
  | 'missing_field'
  | 'duplicate_suspected'
  | 'geocode_failed'
  | 'date_issue'
  | 'invalid_phone'
  | 'invalid_address'
  | 'other';

export interface Organization {
  organization_id: string;
  organization_name: string;
  organization_slug?: string;
  legal_name?: string;
  parent_company_name?: string;
  website_url?: string;
  main_phone?: string;
  general_email?: string;
  organization_description?: string;
  headquarters_city?: string;
  headquarters_state?: string;
  is_active?: boolean;
  created_at?: string;
  updated_at?: string;
}

export interface ServiceType {
  service_type_id: string;
  service_type_name: string;
  service_type_slug?: string;
  description?: string;
  category?: string;
  is_active?: boolean;
  display_order?: number;
  created_at?: string;
  updated_at?: string;
}

export interface Location {
  location_id: string;
  organization_id: string;
  location_name?: string;
  address_line_1: string;
  address_line_2?: string;
  city: string;
  state: string;
  postal_code: string;
  country?: string;
  county?: string;
  latitude?: number;
  longitude?: number;
  public_phone?: string;
  public_email?: string;
  website_url?: string;
  service_area_text?: string;
  accepts_public_display?: boolean;
  listing_status?: ListingStatus;
  last_verified_at?: string;
  first_published_at?: string;
  published_at?: string;
  unpublished_at?: string;
  source_system?: string;
  source_url?: string;
  source_last_seen_at?: string;
  is_sponsored?: boolean;
  is_enhanced?: boolean;
  insurance_accepted?: string[];
  featured_in_alphabetical?: boolean;
  display_order?: number;
  needs_review_flags?: any;
  place_id?: string;
  service_area_description?: string;
  years_in_business?: number;
  availability_24_7?: boolean;
  languages_spoken?: string[];
  special_programs?: string[];
  created_at?: string;
  updated_at?: string;
}

export interface AccreditationRecord {
  accreditation_id: string;
  location_id: string;
  accrediting_body: string;
  accreditation_status: AccreditationStatus;
  accreditation_scope?: string;
  effective_date?: string;
  expiration_date?: string;
  accreditation_reference_code?: string;
  source_url?: string;
  imported_at?: string;
  is_current_record: boolean;
  created_at?: string;
  updated_at?: string;
}

export interface Image {
  image_id: string;
  file_path: string;
  original_filename: string;
  alt_text: string;
  caption?: string;
  title?: string;
  backup_url?: string;
  image_purpose: ImagePurpose;
  width?: number;
  height?: number;
  file_size?: number;
  mime_type?: string;
  optimization_applied: boolean;
  max_width?: number;
  max_height?: number;
  quality?: number;
  uploaded_by?: string;
  created_at: string;
  updated_at: string;
}

export interface Article {
  article_id: string;
  title: string;
  slug: string;
  summary?: string;
  body_content: string;
  content_category: string;
  status: ContentStatus;
  published_at?: string;
  reviewed_at?: string;
  author_name?: string;
  seo_title?: string;
  seo_description?: string;
  featured_image_id?: string;
  created_at: string;
  updated_at: string;
}

export interface SearchLog {
  search_log_id: string;
  query_text?: string;
  interpreted_location?: string;
  interpreted_latitude?: number;
  interpreted_longitude?: number;
  service_type_filters?: string[];
  state_filter?: string;
  city_filter?: string;
  distance_filter_miles?: number;
  result_count: number;
  clicked_location_id?: string;
  session_identifier?: string;
  searched_at: string;
  created_at: string;
}

// Combined types for queries
export interface LocationWithDetails extends Location {
  organization?: Organization;
  service_types?: ServiceType[];
  accreditation_records?: AccreditationRecord[];
  listing_settings?: LocationListingSettings;
}

export interface LocationListingSettings {
  location_listing_settings_id: string;
  location_id: string;
  listing_tier_id: string;
  enhanced_description?: string;
  logo_image_id?: string;
  specialties_text?: string;
  call_to_action_text?: string;
  call_to_action_url?: string;
  is_featured: boolean;
  created_at: string;
  updated_at: string;
}
