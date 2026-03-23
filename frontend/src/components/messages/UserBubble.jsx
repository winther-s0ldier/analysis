import React from 'react';
import { motion } from 'framer-motion';
import { FileBarChart } from 'lucide-react';
import { cn } from '../ui/Badge';

export function UserBubble({ message }) {
  const isFile = message.role === 'user' && message.type === 'file';

  return (
    <motion.div
      className="flex justify-end w-full mb-4"
      initial={{ opacity: 0, y: 10, scale: 0.97 }}
      animate={{ opacity: 1, y: 0, scale: 1 }}
      transition={{ duration: 0.3, ease: [0.165, 0.84, 0.44, 1] }}
    >
      {isFile ? (
        /* File bubble — white card with indigo icon */
        <div
          className="flex items-center gap-3 px-4 py-3 rounded-2xl border max-w-[85%]"
          style={{
            background: '#FFFFFF',
            borderColor: '#E5E7EB',
            boxShadow: '0 1px 4px rgba(0,0,0,0.05)',
            borderTopRightRadius: 4,
          }}
        >
          <div
            className="w-9 h-9 rounded-xl flex items-center justify-center shrink-0"
            style={{
              background: 'rgba(99,102,241,0.1)',
              color: '#6366F1',
            }}
          >
            <FileBarChart size={18} strokeWidth={2} />
          </div>
          <div className="flex flex-col leading-none">
            <span className="text-[13px] font-semibold text-text-primary leading-tight">
              {message.payload.name}
            </span>
            <span
              className="text-[11px] font-mono mt-0.5"
              style={{ color: '#9CA3AF' }}
            >
              {message.payload.size}
            </span>
          </div>
        </div>
      ) : (
        /* Text bubble — dark, minimal */
        <div
          className="px-4 py-2.5 rounded-2xl text-[14.5px] font-medium leading-relaxed whitespace-pre-wrap max-w-[85%]"
          style={{
            background: '#111827',
            color: '#F9FAFB',
            borderTopRightRadius: 4,
            boxShadow: '0 2px 8px rgba(0,0,0,0.12)',
          }}
        >
          {message.payload}
        </div>
      )}
    </motion.div>
  );
}
