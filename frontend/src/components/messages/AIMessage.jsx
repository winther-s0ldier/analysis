import React from 'react';
import { motion } from 'framer-motion';

// Small grid icon rendered inline — no external dependency
function GridIcon({ size = 16, color = '#fff' }) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" aria-hidden="true">
      <rect x="2"  y="2"  width="5" height="5" rx="1.2" fill={color} opacity="0.95" />
      <rect x="9"  y="2"  width="5" height="5" rx="1.2" fill={color} opacity="0.45" />
      <rect x="2"  y="9"  width="5" height="5" rx="1.2" fill={color} opacity="0.45" />
      <rect x="9"  y="9"  width="5" height="5" rx="1.2" fill={color} opacity="0.75" />
    </svg>
  );
}

export function AIMessage({ children }) {
  return (
    <motion.div
      className="flex gap-3 items-start w-full mb-5"
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.35, ease: [0.165, 0.84, 0.44, 1] }}
    >
      {/* Avatar */}
      <div
        className="w-7 h-7 rounded-full flex items-center justify-center shrink-0 mt-0.5"
        style={{
          background: 'linear-gradient(135deg, #6366F1 0%, #4F46E5 100%)',
          boxShadow: '0 2px 8px rgba(99,102,241,0.3)',
        }}
      >
        <GridIcon size={14} color="#fff" />
      </div>

      {/* Content */}
      <div className="flex-1 min-w-0 flex flex-col items-start gap-2.5 pt-0.5">
        {children}
      </div>
    </motion.div>
  );
}

// ── Markdown renderer ────────────────────────────────────────────────────────
// Handles **bold**, *italic*, bullet lines (* text / - text), and paragraphs.
// Pure React elements — no dangerouslySetInnerHTML, XSS-safe.

function InlineMarkdown({ text }) {
  // Split on **bold** and *italic* spans, preserving delimiters as capture groups
  const tokens = text.split(/(\*\*[^*]+\*\*|\*[^*\s][^*]*[^*\s]\*|\*[^*\s]\*)/g);
  return (
    <>
      {tokens.map((tok, i) => {
        if (tok.startsWith('**') && tok.endsWith('**'))
          return <strong key={i} style={{ fontWeight: 600, color: '#111827' }}>{tok.slice(2, -2)}</strong>;
        if (tok.startsWith('*') && tok.endsWith('*') && tok.length > 2)
          return <em key={i}>{tok.slice(1, -1)}</em>;
        return tok;
      })}
    </>
  );
}

function MarkdownText({ text }) {
  if (!text) return null;

  // Normalise: collapse 3+ blank lines → 2, trim
  const normalised = text.replace(/\n{3,}/g, '\n\n').trim();
  // Split into paragraph blocks
  const blocks = normalised.split(/\n\n+/);

  return (
    <div className="text-[14.5px] leading-relaxed text-text-secondary max-w-prose space-y-2.5" style={{ fontWeight: 400 }}>
      {blocks.map((block, bi) => {
        const lines = block.split('\n');

        // Detect bullet block: every non-empty line starts with "* " or "- "
        const bulletLines = lines.filter(l => l.trim());
        const isBulletBlock = bulletLines.length > 0 &&
          bulletLines.every(l => /^\s*[\*\-]\s+/.test(l));

        if (isBulletBlock) {
          return (
            <ul key={bi} className="space-y-1.5 pl-0 list-none">
              {bulletLines.map((line, li) => {
                const content = line.replace(/^\s*[\*\-]\s+/, '');
                return (
                  <li key={li} className="flex items-start gap-2">
                    <span
                      className="shrink-0 w-1.5 h-1.5 rounded-full mt-[6px]"
                      style={{ background: '#6366F1', opacity: 0.6 }}
                    />
                    <span><InlineMarkdown text={content} /></span>
                  </li>
                );
              })}
            </ul>
          );
        }

        // Mixed block: some lines may be bullets, some plain
        // Render line by line
        return (
          <p key={bi} className="m-0">
            {lines.map((line, li) => {
              const isBullet = /^\s*[\*\-]\s+/.test(line);
              if (isBullet) {
                const content = line.replace(/^\s*[\*\-]\s+/, '');
                return (
                  <span key={li} className="flex items-start gap-2 mt-1">
                    <span
                      className="shrink-0 w-1.5 h-1.5 rounded-full mt-[6px]"
                      style={{ background: '#6366F1', opacity: 0.6 }}
                    />
                    <span><InlineMarkdown text={content} /></span>
                  </span>
                );
              }
              return (
                <span key={li}>
                  {li > 0 && !lines[li - 1].trim() ? null : (li > 0 ? ' ' : '')}
                  <InlineMarkdown text={line} />
                </span>
              );
            })}
          </p>
        );
      })}
    </div>
  );
}

export function AITextMessage({ payload }) {
  return (
    <AIMessage>
      {/* MarkdownText renders **bold**, *italic*, and bullet lists as proper React
          elements — no dangerouslySetInnerHTML, fully XSS-safe. */}
      <MarkdownText text={payload} />
    </AIMessage>
  );
}

// ── Thinking bubble ──────────────────────────────────────────────────────────
// Shown while waiting for a chat / Ask AI response from the backend.

export function ThinkingBubble() {
  return (
    <AIMessage>
      <div className="flex items-center gap-1.5 py-1 px-0.5">
        {[0, 1, 2].map(i => (
          <motion.div
            key={i}
            className="w-2 h-2 rounded-full"
            style={{ background: '#9CA3AF' }}
            animate={{ y: [0, -5, 0], opacity: [0.5, 1, 0.5] }}
            transition={{ duration: 0.8, repeat: Infinity, delay: i * 0.15, ease: 'easeInOut' }}
          />
        ))}
      </div>
    </AIMessage>
  );
}
