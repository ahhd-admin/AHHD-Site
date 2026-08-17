import { useState, useEffect } from 'react';
import {
  Home,
  Users,
  FileText,
  AlertCircle,
  CheckCircle,
  Clock,
  Search,
  Image as ImageIcon,
  LogOut
} from 'lucide-react';
import { supabase } from '../lib/supabase';
import { useAuth } from '../contexts/AuthContext';

interface DashboardStats {
  totalLocations: number;
  publishedLocations: number;
  needsReview: number;
  expiringSoon: number;
}

export default function AdminDashboard() {
  const [stats, setStats] = useState<DashboardStats>({
    totalLocations: 0,
    publishedLocations: 0,
    needsReview: 0,
    expiringSoon: 0
  });
  const [loading, setLoading] = useState(true);
  const { signOut } = useAuth();

  const handleSignOut = async () => {
    await signOut();
    window.history.pushState({}, '', '/admin');
    window.location.reload();
  };

  useEffect(() => {
    loadDashboardStats();
  }, []);

  const loadDashboardStats = async () => {
    setLoading(true);

    const [locationsRes, publishedRes, reviewRes, accreditationsRes] = await Promise.all([
      supabase.from('locations').select('location_id', { count: 'exact', head: true }),
      supabase.from('locations').select('location_id', { count: 'exact', head: true }).eq('listing_status', 'published'),
      supabase.from('locations').select('location_id', { count: 'exact', head: true }).eq('listing_status', 'needs_review'),
      supabase.from('accreditation_records').select('accreditation_id', { count: 'exact', head: true }).gte('expiration_date', new Date().toISOString()).lte('expiration_date', new Date(Date.now() + 90 * 24 * 60 * 60 * 1000).toISOString())
    ]);

    setStats({
      totalLocations: locationsRes.count || 0,
      publishedLocations: publishedRes.count || 0,
      needsReview: reviewRes.count || 0,
      expiringSoon: accreditationsRes.count || 0
    });

    setLoading(false);
  };

  return (
    <div className="min-h-screen bg-neutral-50">
      <header className="bg-navy-800 text-white shadow-lg">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-heading font-bold text-white">AHHD Admin Dashboard</h1>
              <p className="text-primary-300 mt-1">Manage directory listings and content</p>
            </div>
            <div className="flex gap-3">
              <a href="/" className="btn bg-primary-500 text-white hover:bg-primary-600">
                <Home className="w-5 h-5 mr-2 inline" aria-hidden="true" />
                View Site
              </a>
              <button
                onClick={handleSignOut}
                className="btn bg-slate-700 text-white hover:bg-slate-600"
              >
                <LogOut className="w-5 h-5 mr-2 inline" aria-hidden="true" />
                Sign Out
              </button>
            </div>
          </div>
        </div>
      </header>

      <main id="main-content" className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-12">
          <div className="card p-6">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-medium text-neutral-600">Total Listings</h3>
              <FileText className="w-5 h-5 text-primary-600" aria-hidden="true" />
            </div>
            <p className="text-3xl font-heading font-bold text-navy-800">
              {loading ? '-' : stats.totalLocations}
            </p>
          </div>

          <div className="card p-6">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-medium text-neutral-600">Published</h3>
              <CheckCircle className="w-5 h-5 text-success-600" aria-hidden="true" />
            </div>
            <p className="text-3xl font-heading font-bold text-navy-800">
              {loading ? '-' : stats.publishedLocations}
            </p>
          </div>

          <div className="card p-6">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-medium text-neutral-600">Needs Review</h3>
              <AlertCircle className="w-5 h-5 text-warning-600" aria-hidden="true" />
            </div>
            <p className="text-3xl font-heading font-bold text-navy-800">
              {loading ? '-' : stats.needsReview}
            </p>
          </div>

          <div className="card p-6">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-medium text-neutral-600">Expiring Soon</h3>
              <Clock className="w-5 h-5 text-error-600" aria-hidden="true" />
            </div>
            <p className="text-3xl font-heading font-bold text-navy-800">
              {loading ? '-' : stats.expiringSoon}
            </p>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          <a href="/admin/locations" className="card-hover p-8 block group">
            <div className="flex items-center gap-4 mb-4">
              <div className="p-3 bg-primary-100 rounded-lg group-hover:bg-primary-200 transition-colors">
                <FileText className="w-8 h-8 text-primary-600" aria-hidden="true" />
              </div>
              <div>
                <h3 className="text-xl font-heading font-semibold text-navy-800">Manage Listings</h3>
                <p className="text-sm text-neutral-600">View and edit provider locations</p>
              </div>
            </div>
          </a>

          <a href="/admin/review-queue" className="card-hover p-8 block group relative">
            <div className="flex items-center gap-4 mb-4">
              <div className="p-3 bg-warning-100 rounded-lg group-hover:bg-warning-200 transition-colors">
                <AlertCircle className="w-8 h-8 text-warning-600" aria-hidden="true" />
              </div>
              <div className="flex-1">
                <h3 className="text-xl font-heading font-semibold text-navy-800">Review Queue</h3>
                <p className="text-sm text-neutral-600">Items needing attention</p>
              </div>
              {stats.needsReview > 0 && (
                <div className="absolute top-6 right-6 px-3 py-1 bg-red-600 text-white rounded-full text-sm font-bold">
                  {stats.needsReview}
                </div>
              )}
            </div>
          </a>

          <a href="/admin/organizations" className="card-hover p-8 block group">
            <div className="flex items-center gap-4 mb-4">
              <div className="p-3 bg-navy-100 rounded-lg group-hover:bg-navy-200 transition-colors">
                <Users className="w-8 h-8 text-navy-600" aria-hidden="true" />
              </div>
              <div>
                <h3 className="text-xl font-heading font-semibold text-navy-800">Organizations</h3>
                <p className="text-sm text-neutral-600">Manage provider organizations</p>
              </div>
            </div>
          </a>

          <a href="/admin/images" className="card-hover p-8 block group">
            <div className="flex items-center gap-4 mb-4">
              <div className="p-3 bg-success-100 rounded-lg group-hover:bg-success-200 transition-colors">
                <ImageIcon className="w-8 h-8 text-success-600" aria-hidden="true" />
              </div>
              <div>
                <h3 className="text-xl font-heading font-semibold text-navy-800">Images</h3>
                <p className="text-sm text-neutral-600">Manage site images</p>
              </div>
            </div>
          </a>

          <a href="/admin/content" className="card-hover p-8 block group">
            <div className="flex items-center gap-4 mb-4">
              <div className="p-3 bg-primary-100 rounded-lg group-hover:bg-primary-200 transition-colors">
                <FileText className="w-8 h-8 text-primary-600" aria-hidden="true" />
              </div>
              <div>
                <h3 className="text-xl font-heading font-semibold text-navy-800">Content</h3>
                <p className="text-sm text-neutral-600">Manage articles and resources</p>
              </div>
            </div>
          </a>

          <a href="/admin/search-logs" className="card-hover p-8 block group">
            <div className="flex items-center gap-4 mb-4">
              <div className="p-3 bg-neutral-200 rounded-lg group-hover:bg-neutral-300 transition-colors">
                <Search className="w-8 h-8 text-neutral-600" aria-hidden="true" />
              </div>
              <div>
                <h3 className="text-xl font-heading font-semibold text-navy-800">Search Analytics</h3>
                <p className="text-sm text-neutral-600">View search insights</p>
              </div>
            </div>
          </a>
        </div>

        <div className="mt-12 card p-8">
          <h2 className="text-2xl font-heading font-semibold text-navy-800 mb-4">
            Quick Start Guide
          </h2>
          <div className="space-y-4">
            <div className="flex items-start gap-3">
              <CheckCircle className="w-5 h-5 text-success-600 flex-shrink-0 mt-1" aria-hidden="true" />
              <div>
                <p className="font-medium text-navy-800">Database Setup Complete</p>
                <p className="text-sm text-neutral-600">All core tables and relationships have been created</p>
              </div>
            </div>
            <div className="flex items-start gap-3">
              <CheckCircle className="w-5 h-5 text-success-600 flex-shrink-0 mt-1" aria-hidden="true" />
              <div>
                <p className="font-medium text-navy-800">Keep-Alive System Active</p>
                <p className="text-sm text-neutral-600">Hourly database pings are configured</p>
              </div>
            </div>
            <div className="flex items-start gap-3">
              <CheckCircle className="w-5 h-5 text-success-600 flex-shrink-0 mt-1" aria-hidden="true" />
              <div>
                <p className="font-medium text-navy-800">Image Optimization Ready</p>
                <p className="text-sm text-neutral-600">Upload images with automatic optimization based on purpose</p>
              </div>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
