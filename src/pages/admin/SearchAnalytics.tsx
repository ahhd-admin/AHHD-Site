import { Search, TrendingUp, LogOut } from 'lucide-react';
import { useAuth } from '../../contexts/AuthContext';

export default function SearchAnalytics() {
  const { signOut } = useAuth();

  const handleSignOut = async () => {
    await signOut();
    window.location.href = '/admin';
  };
  return (
    <div className="min-h-screen bg-neutral-50">
      <header className="bg-navy-800 text-white shadow-lg">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-heading font-bold text-white">Search Analytics</h1>
              <p className="text-primary-300 mt-1">View search insights</p>
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
        <div className="card p-12 text-center">
          <div className="flex items-center justify-center gap-3 mb-4">
            <Search className="w-16 h-16 text-neutral-400" aria-hidden="true" />
            <TrendingUp className="w-12 h-12 text-primary-400" aria-hidden="true" />
          </div>
          <h2 className="text-xl font-heading font-semibold text-navy-800 mb-2">Search Analytics</h2>
          <p className="text-neutral-600">Track popular searches and user behavior patterns</p>
        </div>
      </main>
    </div>
  );
}
