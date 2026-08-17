import { useState, useEffect } from 'react';
import { FileText, Search, CreditCard as Edit, Eye, Plus, LogOut } from 'lucide-react';
import { supabase } from '../../lib/supabase';
import ListingFormWizard from '../../components/ListingFormWizard';
import { useAuth } from '../../contexts/AuthContext';
import { buildProviderSlug } from '../../lib/slug';

interface Location {
  location_id: string;
  location_name: string;
  achc_source_id?: string;
  city: string;
  state: string;
  listing_status: string;
  last_verified_at: string;
  organization?: {
    organization_name: string;
  };
}

export default function ManageListings() {
  const [locations, setLocations] = useState<Location[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [showForm, setShowForm] = useState(false);
  const [editingLocationId, setEditingLocationId] = useState<string | undefined>(undefined);
  const { signOut } = useAuth();

  const handleSignOut = async () => {
    await signOut();
    window.history.pushState({}, '', '/admin');
    window.location.reload();
  };

  useEffect(() => {
    loadLocations();
  }, []);

  const loadLocations = async () => {
    setLoading(true);
    const { data, error } = await supabase
      .from('locations')
      .select(`
        location_id,
        location_name,
        achc_source_id,
        city,
        state,
        listing_status,
        last_verified_at,
        organization:organizations(organization_name)
      `)
      .order('location_name');

    if (data && !error) {
      setLocations(data.map(loc => ({
        ...loc,
        organization: Array.isArray(loc.organization) ? loc.organization[0] : loc.organization
      })));
    }
    setLoading(false);
  };

  const handleAddNew = () => {
    setEditingLocationId(undefined);
    setShowForm(true);
  };

  const handleEdit = (locationId: string) => {
    setEditingLocationId(locationId);
    setShowForm(true);
  };

  const handleCloseForm = () => {
    setShowForm(false);
    setEditingLocationId(undefined);
    loadLocations();
  };

  const handleSave = () => {
    setShowForm(false);
    setEditingLocationId(undefined);
    loadLocations();
  };

  const filteredLocations = locations.filter(loc => {
    const search = searchTerm.toLowerCase();
    return (
      (loc.location_name?.toLowerCase() || '').includes(search) ||
      loc.city.toLowerCase().includes(search) ||
      loc.organization?.organization_name?.toLowerCase().includes(search) ||
      loc.postal_code.includes(search)
    );
  });

  return (
    <div className="min-h-screen bg-neutral-50">
      <header className="bg-navy-800 text-white shadow-lg">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-heading font-bold text-white">Manage Listings</h1>
              <p className="text-primary-300 mt-1">View and edit provider locations</p>
            </div>
            <div className="flex gap-3">
              <button
                onClick={handleAddNew}
                className="bg-primary-600 hover:bg-primary-700 text-white px-4 py-2 rounded-lg font-medium transition-colors flex items-center gap-2"
              >
                <Plus className="w-4 h-4" aria-hidden="true" />
                Add New Listing
              </button>
              <a href="/admin/dashboard" className="bg-neutral-700 text-white hover:bg-neutral-600 px-4 py-2 rounded-lg font-medium transition-colors">
                Back to Dashboard
              </a>
              <button
                onClick={handleSignOut}
                className="bg-slate-700 text-white hover:bg-slate-600 px-4 py-2 rounded-lg font-medium transition-colors flex items-center gap-2"
              >
                <LogOut className="w-4 h-4" aria-hidden="true" />
                Sign Out
              </button>
            </div>
          </div>
        </div>
      </header>

      <main id="main-content" className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
        <div className="mb-6">
          <div className="relative">
            <Search className="absolute left-4 top-1/2 -translate-y-1/2 text-neutral-500 w-5 h-5" aria-hidden="true" />
            <input
              type="text"
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              placeholder="Search by location name or city..."
              className="w-full pl-12 pr-4 py-3 border-2 border-neutral-500 rounded-xl focus:outline-none focus:ring-4 focus:ring-primary-200 focus:border-primary-500 transition-all"
            />
          </div>
        </div>

        {loading ? (
          <div className="text-center py-12">
            <p className="text-neutral-600">Loading locations...</p>
          </div>
        ) : filteredLocations.length === 0 ? (
          <div className="card p-8 text-center">
            <FileText className="w-12 h-12 text-neutral-400 mx-auto mb-4" aria-hidden="true" />
            <p className="text-neutral-600">No locations found</p>
          </div>
        ) : (
          <div className="space-y-4">
            {filteredLocations.map((location) => (
              <div key={location.location_id} className="card p-6 hover:shadow-lg transition-shadow">
                <div className="flex items-center justify-between">
                  <div className="flex-1">
                    <h3 className="text-lg font-heading font-semibold text-navy-800 mb-1">
                      {location.organization?.organization_name || location.location_name}
                    </h3>
                    <p className="text-neutral-600">
                      {location.city}, {location.state}
                    </p>
                    <div className="mt-2 flex items-center gap-4 text-sm">
                      <span className={`px-3 py-1 rounded-full font-medium ${
                        location.listing_status === 'published'
                          ? 'bg-success-100 text-success-700'
                          : location.listing_status === 'needs_review'
                          ? 'bg-warning-100 text-warning-700'
                          : 'bg-neutral-200 text-neutral-700'
                      }`}>
                        {location.listing_status.replace('_', ' ')}
                      </span>
                      {location.last_verified_at && (
                        <span className="text-neutral-500">
                          Last verified: {new Date(location.last_verified_at).toLocaleDateString()}
                        </span>
                      )}
                    </div>
                  </div>
                  <div className="flex gap-2">
                    <a
                      href={`/provider/${buildProviderSlug(location.organization?.organization_name || location.location_name || '', location.achc_source_id)}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="border-2 border-neutral-500 text-neutral-700 hover:bg-neutral-50 px-4 py-2 rounded-lg font-medium transition-colors flex items-center gap-2"
                    >
                      <Eye className="w-4 h-4" aria-hidden="true" />
                      View
                    </a>
                    <button
                      onClick={() => handleEdit(location.location_id)}
                      className="bg-primary-600 hover:bg-primary-700 text-white px-4 py-2 rounded-lg font-medium transition-colors flex items-center gap-2"
                    >
                      <Edit className="w-4 h-4" aria-hidden="true" />
                      Edit
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </main>

      {showForm && (
        <ListingFormWizard
          locationId={editingLocationId}
          onClose={handleCloseForm}
          onSave={handleSave}
        />
      )}
    </div>
  );
}
