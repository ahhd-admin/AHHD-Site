import { useState, useEffect } from 'react';
import { Users, Search, Building2, LogOut } from 'lucide-react';
import { supabase } from '../../lib/supabase';
import { useAuth } from '../../contexts/AuthContext';

interface Organization {
  organization_id: string;
  organization_name: string;
  website_url: string | null;
  main_phone: string | null;
  is_active: boolean;
}

export default function ManageOrganizations() {
  const [organizations, setOrganizations] = useState<Organization[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const { signOut } = useAuth();

  const handleSignOut = async () => {
    await signOut();
    window.location.href = '/admin';
  };

  useEffect(() => {
    loadOrganizations();
  }, []);

  const loadOrganizations = async () => {
    setLoading(true);
    const { data, error } = await supabase
      .from('organizations')
      .select('organization_id, organization_name, website_url, main_phone, is_active')
      .order('organization_name');

    if (data && !error) {
      setOrganizations(data);
    }
    setLoading(false);
  };

  const filteredOrgs = organizations.filter(org =>
    org.organization_name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  return (
    <div className="min-h-screen bg-neutral-50">
      <header className="bg-navy-800 text-white shadow-lg">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-heading font-bold text-white">Organizations</h1>
              <p className="text-primary-300 mt-1">Manage provider organizations</p>
            </div>
            <div className="flex gap-3">
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
              placeholder="Search organizations..."
              className="w-full pl-12 pr-4 py-3 border-2 border-neutral-500 rounded-xl focus:outline-none focus:ring-4 focus:ring-primary-200 focus:border-primary-500 transition-all"
            />
          </div>
        </div>

        {loading ? (
          <div className="text-center py-12">
            <p className="text-neutral-600">Loading organizations...</p>
          </div>
        ) : filteredOrgs.length === 0 ? (
          <div className="card p-8 text-center">
            <Building2 className="w-12 h-12 text-neutral-400 mx-auto mb-4" aria-hidden="true" />
            <p className="text-neutral-600">No organizations found</p>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {filteredOrgs.map((org) => (
              <div key={org.organization_id} className="card p-6 hover:shadow-lg transition-shadow">
                <div className="flex items-start gap-4">
                  <div className="p-3 bg-navy-100 rounded-lg flex-shrink-0">
                    <Building2 className="w-6 h-6 text-navy-600" aria-hidden="true" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <h3 className="text-lg font-heading font-semibold text-navy-800 mb-2 truncate">
                      {org.organization_name}
                    </h3>
                    {org.website_url && (
                      <a
                        href={org.website_url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-sm text-primary-600 hover:text-primary-700 block mb-1"
                      >
                        {org.website_url}
                      </a>
                    )}
                    {org.main_phone && (
                      <p className="text-sm text-neutral-600">{org.main_phone}</p>
                    )}
                    <div className="mt-3">
                      <span className={`text-xs px-2 py-1 rounded-full font-medium ${
                        org.is_active
                          ? 'bg-success-100 text-success-700'
                          : 'bg-neutral-200 text-neutral-700'
                      }`}>
                        {org.is_active ? 'Active' : 'Inactive'}
                      </span>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </main>
    </div>
  );
}
