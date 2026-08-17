import { FileText, LogOut } from 'lucide-react';
import { useAuth } from '../../contexts/AuthContext';

export default function ManageContent() {
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
              <h1 className="text-2xl font-heading font-bold text-white">Content Management</h1>
              <p className="text-primary-300 mt-1">Manage articles and resources</p>
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
          <FileText className="w-16 h-16 text-neutral-400 mx-auto mb-4" aria-hidden="true" />
          <h2 className="text-xl font-heading font-semibold text-navy-800 mb-2">Content Management</h2>
          <p className="text-neutral-600">Create and edit educational articles and resources</p>
        </div>
      </main>
    </div>
  );
}
