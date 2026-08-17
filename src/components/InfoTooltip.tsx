import { Info } from 'lucide-react';
import { useState } from 'react';

interface InfoTooltipProps {
  content: string;
}

export default function InfoTooltip({ content }: InfoTooltipProps) {
  const [show, setShow] = useState(false);

  return (
    <div className="relative inline-block">
      <button
        type="button"
        onMouseEnter={() => setShow(true)}
        onMouseLeave={() => setShow(false)}
        onClick={(e) => {
          e.preventDefault();
          setShow(!show);
        }}
        className="text-neutral-500 hover:text-neutral-600 transition-colors ml-1"
        aria-label="More information"
        aria-expanded={show}
      >
        <Info className="w-4 h-4" aria-hidden="true" />
      </button>
      {show && (
        <div className="absolute left-1/2 -translate-x-1/2 bottom-full mb-2 w-64 bg-navy-800 text-white text-xs rounded-lg p-3 shadow-xl z-50">
          {content}
          <div className="absolute top-full left-1/2 -translate-x-1/2 -mt-1 border-4 border-transparent border-t-navy-800" />
        </div>
      )}
    </div>
  );
}
